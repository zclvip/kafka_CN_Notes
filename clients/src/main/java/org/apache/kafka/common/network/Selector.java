/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 */
public class Selector implements Selectable, AutoCloseable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;

    private enum CloseMode {
        //处理未完成的 staged receives，通知disconnect
        GRACEFUL(true),            // process outstanding staged receives, notify disconnect
        //丢弃未完成的receives,通知disconnect
        NOTIFY_ONLY(true),         // discard any outstanding receives, notify disconnect
        //丢弃未完成的 receives,不通知disconnect
        DISCARD_NO_NOTIFY(false);  // discard any outstanding receives, no disconnect notification

        boolean notifyDisconnect;

        CloseMode(boolean notifyDisconnect) {
            this.notifyDisconnect = notifyDisconnect;
        }
    }

    private final Logger log;
    //java 的 selector，用来监听网络I/O事件
    private final java.nio.channels.Selector nioSelector;
    //维护了NodeId和kafakChannel之间的映射关系，表示生产者和各个node之间的网络连接，这些kafakChannel都由ChannelBuilder接口创建
    // 在创建channel之后就添加了进去，在close方法中移除
    private final Map<String, KafkaChannel> channels;
    private final Set<KafkaChannel> explicitlyMutedChannels;
    private boolean outOfMemory;
    //记录已经完全发送出去的请求，在写事件的时候添加进行
    private final List<Send> completedSends;
    //记录已经完全接收到的请求，从stagedReceives中转移而来，在一次poll操作中，至多一个stagedReceives被添加进来
    private final List<NetworkReceive> completedReceives;
    //暂存一次OP_READ事件处理过程中读取到的全部请求。当一次读事件处理完成之后，会将stageReceives集合中第一个请求保存到completedReceives中
    // 在一次poll中，当stagedReceives中有值时，timeout=0，select操作立即返回，如果有准备好的事件就处理事件，如果没有，就转移第一个response到completedReceives，即已经有了读取的response，不进行具体的读事件，
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    private final Set<SelectionKey> immediatelyConnectedKeys;
    //在close方法，如果正确关闭并且队列请求还没发送完的情况下，才保存进该集合，是一种特殊状态
    private final Map<String, KafkaChannel> closingChannels;
    //如果是明文，set中不会有值，如果是ssl的话，就起作用了
    private Set<SelectionKey> keysWithBufferedRead;
    //记录一次poll过程中发现的断开的连接
    private final Map<String, ChannelState> disconnected;
    //记录一次poll过程中发现的新建立的连接,在close方法中移除
    private final List<String> connected;
    //记录向那些node发送的请求失败了
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    //用于创建KafkaChannel的builder。根据不同的配置创建不同的TransportLayer的子类，然后创建KafkaChannel
    private final ChannelBuilder channelBuilder;
    private final int maxReceiveSize;
    //是否记录每个连接的执行时间
    private final boolean recordTimePerConnection;
    private final IdleExpiryManager idleExpiryManager;
    private final MemoryPool memoryPool;
    private final long lowMemThreshold;
    //indicates if the previous call to poll was able to make progress in reading already-buffered data.
    //this is used to prevent tight loops when memory is not available to read any more data
    //为了阻止过于频繁的循环，内存不足时，再频繁也没用
    private boolean madeReadProgressLastPoll = true;

    /**
     * Create a new nioSelector
     * @param maxReceiveSize Max size in bytes of a single network receive (use {@link NetworkReceive#UNLIMITED} for no limit)
     * @param connectionMaxIdleMs Max idle connection time (use {@link #NO_IDLE_TIMEOUT_MS} to disable idle timeout)
     * @param metrics Registry for Selector metrics
     * @param time Time implementation
     * @param metricGrpPrefix Prefix for the group of metrics registered by Selector
     * @param metricTags Additional tags to add to metrics registered by Selector
     * @param metricsPerConnection Whether or not to enable per-connection metrics
     * @param channelBuilder Channel builder for every new connection
     * @param logContext Context for logging with additional info
     */
    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            boolean recordTimePerConnection,
            ChannelBuilder channelBuilder,
            MemoryPool memoryPool,
            LogContext logContext) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.time = time;
        this.channels = new HashMap<>();
        this.explicitlyMutedChannels = new HashSet<>();
        this.outOfMemory = false;
        this.completedSends = new ArrayList<>();
        this.completedReceives = new ArrayList<>();
        this.stagedReceives = new HashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.closingChannels = new HashMap<>();
        this.keysWithBufferedRead = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new HashMap<>();
        this.failedSends = new ArrayList<>();
        this.sensors = new SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection);
        this.channelBuilder = channelBuilder;
        this.recordTimePerConnection = recordTimePerConnection;
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);
        this.memoryPool = memoryPool;
        //低内存的值是内存池的0.1倍
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        this.log = logContext.logger(Selector.class);
    }

    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            ChannelBuilder channelBuilder,
            LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.<String, String>emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    //创立连接，仅仅是初始化连接，或者说只是注册了一个连接事件，连接完成是在poll方法中
    // 通过KSelector.finishConnect()的检查如果连接成功，那么保存到 connected集合中方法中
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureNotRegistered(id);
        //创建SocketChannel
        SocketChannel socketChannel = SocketChannel.open();
        try {
            //配置SocketChannel 设置为长连接和非阻塞模式 设置了发送和接收缓存区大小，设置了禁用缓存延迟功能
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize);
            //因为是非阻塞模式，所有SocketChannel.connect()方法是发起一个连接
            //connect方法在连接正式建立之前就可能返回，在后面会通过KSelector.finishConnect()方法确认连接是否真的建立了
            boolean connected = doConnect(socketChannel, address);
            //注册channel，并关注连接事件，生成KafkaChannel，并保存到channels中进行管理
            SelectionKey key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT);

            // 在非阻塞模式下socketChannel.configureBlocking(false);connected应该不可能为true
            // 阻塞模式应该可以channel.configureBlocking(true);
            if (connected) {
                // OP_CONNECT won't trigger for immediately connected channels
                //OP_CONNECT事件不会触发immediately connected channels
                log.debug("Immediately connected to node {}", id);
                immediatelyConnectedKeys.add(key);
                //0代表对任何事件都不感兴趣，暂时的
                key.interestOps(0);
            }
        } catch (IOException | RuntimeException e) {
            socketChannel.close();
            throw e;
        }
    }

    // Visible to allow test cases to override. In particular, we use this to implement a blocking connect
    // in order to simulate "immediately connected" sockets.
    protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
        try {
            return channel.connect(address);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Can't resolve address: " + address, e);
        }
    }

    //配置SocketChannel
    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        socketChannel.configureBlocking(false);//设置为非阻塞模式
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);//设置为长连接
        //设置SO_SNDBUF
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        //设置SO_RECEIVW
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        //TCP协议将数据缓存起来直到足够多时一次发送，以避免发送过小的数据包而浪费网络资源
        //true 表示禁用缓存延迟功能  为了消息的实时性
        socket.setTcpNoDelay(true);
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * <p>
     * If a connection already exists with the same connection id in `channels` or `closingChannels`,
     * an exception is thrown. Connection ids must be chosen to avoid conflict when remote ports are reused.
     * Kafka brokers add an incrementing index to the connection id to avoid reuse in the timing window
     * where an existing connection may not yet have been closed by the broker when a new connection with
     * the same remote host:port is processed.
     * </p><p>
     * If a `KafkaChannel` cannot be created for this connection, the `socketChannel` is closed
     * and its selection key cancelled.
     * </p>
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        ensureNotRegistered(id);
        registerChannel(id, socketChannel, SelectionKey.OP_READ);
        this.sensors.connectionCreated.record();
    }

    private void ensureNotRegistered(String id) {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    //注册
    private SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
        //将这个socketc注册到nioSelector上，并关注OP_CONNECT事件
        SelectionKey key = socketChannel.register(nioSelector, interestedOps);
        //创建KafkaChannel 并将该channel附加到key上
        KafkaChannel channel = buildAndAttachKafkaChannel(socketChannel, id, key);
        //保存该KafkaChannel到channels集合
        this.channels.put(id, channel);
        return key;
    }

    //创建kafkaChannel，并将该channel注册到key上
    private KafkaChannel buildAndAttachKafkaChannel(SocketChannel socketChannel, String id, SelectionKey key) throws IOException {
        try {
            KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize, memoryPool);
            key.attach(channel);
            return channel;
        } catch (Exception e) {
            try {
                socketChannel.close();
            } finally {
                key.cancel();
            }
            throw new IOException("Channel could not be created for socket " + socketChannel, e);
        }
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    //阻塞在select方法上的线程会立马返回
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections)
            close(id);
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            log.error("Exception closing nioSelector:", e);
        }
        sensors.close();
        channelBuilder.close();
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     * @param send The request to send
     */
    //将之前创建的ProducerRequest报文对象缓存到KafkaChannel的send字段中，并开始关注此链接的OP_WRITE事件，并没有发生网络I/O
    // 两种情况会在failedSends中保存nodeid，
    // 1、当前的channel正在关闭，即closingChannels中有给nodeid的值，这时候就不能发送数据了，把 nodeid记录在发送失败的集合中，即failedSends
    // 2、channel正常，但是channel.setSend异常了，即正在发送数据，这时候就不能再发送数据了，也会把该nodeid记录在failedSends中
    public void send(Send send) {
        //获取nodeid
        String connectionId = send.destination();
        //通过nodeid获取kafkac
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
        //该nodeid已经有了关闭的KafkaChannel 则记录nodeid到failedSends中
        if (closingChannels.containsKey(connectionId)) {
            // ensure notification via `disconnected`, leave channel in the state in which closing was triggered
            this.failedSends.add(connectionId);
        } else {
            try {
                //暂存send数据到KafkaChannel的send中
                channel.setSend(send);
            } catch (Exception e) {
                // update the state for consistency, the channel will be discarded after `close`
                //暂存send的时候，如果出现异常，状态为faile_send,这个时候的异常最可能就是send还有未发送的数据
                channel.state(ChannelState.FAILED_SEND);
                // ensure notification via `disconnected` when `failedSends` are processed in the next poll
                //出现异常，也把该nodeid记录到failedSends集合中
                this.failedSends.add(connectionId);
                close(channel, CloseMode.DISCARD_NO_NOTIFY);
                if (!(e instanceof CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception {}",
                            connectionId, e);
                    throw e;
                }
            }
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we've have additional bytes
     * in the buffer. To overcome this issue we added "stagedReceives" map which contains per-channel deque. When we are
     * reading a channel we read as many responses as we can and store them into "stagedReceives" and pop one response during
     * the poll to add the completedReceives. If there are any active channels in the "stagedReceives" we set "timeout" to 0
     * and pop response and add to the completedReceives.
     *
     * Atmost one entry is added to "completedReceives" for a channel in each poll. This is necessary to guarantee that
     * requests from a channel are processed on the broker in the order they are sent. Since outstanding requests added
     * by SocketServer to the request queue may be processed by different request handler threads, requests on each
     * channel must be processed one-at-a-time to guarantee ordering.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    /**
     * 该方法可以在每个连接上非阻塞的处理任务I/O事件，包括完成连接，断开连接，创建一个新的send，
     * 在poll方法执行完成之后，我们可以用completeSends、completeReceives、connected、disconnected 集合来确定完成的发送和接收消息，连接和断开连接等，
     * 在poll方法之前会情况这些集合，在poll执行完成会重新生成。
     *
     * 在明文情况下，我们用SocketChannel来读/写网络请求，但是在SSL情况下，在用SocketChannel 向网络发送数据之前需要加密数据，在返回响应response之前需要解密数据。
     * 这需要在我们从网络读取数据的时候维护额外的缓冲区，因为线上数据是加密的，因此我们不能像kafka的协议要求的那样准确的读取到字节。
     * 我们读取尽可能多的字节，直到SSLEngine的应用程序缓冲区大小，这就意味着我们可能会读取比请求的大小更多的字节。
     * 如果SocketChannel上没有更多的数据可以读，selector不会调用该channel，缓冲区中有额外的字节
     * 为解决这个问题，增加了 stagedReceives ,每个channel对应一个队列。当读一个channel时，尽可能多的读取response，然后存储在stagedReceives中,
     * 在poll中pop一个response添加到 completeReceives 集合中，
     *  如果stagedReceives中有有效的channel，设置 timeout=0，并pop一个response添加到 completeReceives 中。
     *
     * 一个channel在每一此poll中最多添加一个response到 completeReceives中。这是为了保证按照broker的发送顺序进行请求处理。
     * 由于socketServer向请求队列添加的未完成的请求可以被不同的请求处理线程处理，为保证顺序，因此每一个channel的请求必须一次处理一个（one-at-a-time）
     *
     * @param timeout The amount of time to block if there is nothing to do
     * @throws IOException
     */
    //真正执行网络I/O的地方
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        boolean madeReadProgressLastCall = madeReadProgressLastPoll;
        //清空上次poll的数据
        clear();

        //数据是否已经缓存了,明文dataInBuffers为false
        boolean dataInBuffers = !keysWithBufferedRead.isEmpty();

        //这几个条件只要有一个成立，那么timeout=0,那么select(timeout)  就是非阻塞，立即返回
        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
            timeout = 0;

        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            //we have recovered from memory pressure. unmute any channel not explicitly muted for other reasons
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute();
                }
            }
            outOfMemory = false;
        }

        /* check ready keys */
        long startSelect = time.nanoseconds();
        //看看有多少通道就绪
        int numReadyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
            Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

            // Poll from channels that have buffered data (but nothing more from the underlying socket)
            //处理已经缓存的数据，明文不走这部分逻辑
            if (dataInBuffers) {
                keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
                Set<SelectionKey> toPoll = keysWithBufferedRead;
                keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
                pollSelectionKeys(toPoll, false, endSelect);
            }

            // Poll from channels where the underlying socket has more data
            pollSelectionKeys(readyKeys, false, endSelect);
            // Clear all selected keys so that they are included in the ready count for the next select
            //处理完事件后要清除事件，要不会死循环
            readyKeys.clear();

            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
            immediatelyConnectedKeys.clear();
        } else {
            madeReadProgressLastPoll = true; //no work is also "progress"
        }

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        //关闭长期空闲的连接
        maybeCloseOldestConnection(endSelect);

        // Add to completedReceives after closing expired connections to avoid removing
        // channels with completed receives until all staged receives are completed.

        //stagedReceives转移到completedReceives
        addToCompletedReceives();
    }

    /**
     * handle any ready I/O on a set of selection keys
     * @param selectionKeys set of keys to handle
     * @param isImmediatelyConnected true if running over a set of keys for just-connected sockets
     * @param currentTimeNanos time at which set of keys was determined
     */
    // package-private for testing
    //处理I/O事件的核心方法，会处理OP_CONNECT、OP_READ、OP_WRITE事件
    void pollSelectionKeys(Set<SelectionKey> selectionKeys,
                           boolean isImmediatelyConnected,
                           long currentTimeNanos) {
        //循环处理事件，determineHandlingOrder在低内存时进行了随机处理
        for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
            //从key中获取KafkaChannel
            KafkaChannel channel = channel(key);
            long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(channel.id());
            //更新lru信息，lru中记录了各个连接的使用情况
            if (idleExpiryManager != null)
                idleExpiryManager.update(channel.id(), currentTimeNanos);

            boolean sendFailed = false;
            try {

                /* complete any connections that have finished their handshake (either normally or immediately) */
                //isImmediatelyConnected 为true，则表示connect返回的是true
                //或者处理OP_CONNECT事件
                if (isImmediatelyConnected || key.isConnectable()) {
                    //finishConnect 检测sockchannel是否建立完成，完成后，会取消连接事件，关注读事件，状态设置为ready
                    if (channel.finishConnect()) {
                        //连接已经建立，nodeid保存到连接已建立集合 connected中
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                channel.id());
                    } else
                        //连接未完成，则跳过对此channel的后续处理
                        continue;
                }

                /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready()) {
                    try {
                        //身份验证
                        channel.prepare();
                    } catch (AuthenticationException e) {
                        sensors.failedAuthentication.record();
                        throw e;
                    }
                    if (channel.ready())
                        sensors.successfulAuthentication.record();
                }

                //处理读事件
                attemptRead(key, channel);
                //明文为false
                if (channel.hasBytesBuffered()) {
                    //this channel has bytes enqueued in intermediary buffers that we could not read
                    //(possibly because no memory). it may be the case that the underlying socket will
                    //not come up in the next poll() and so we need to remember this channel for the
                    //next poll call otherwise data may be stuck in said buffers forever. If we attempt
                    //to process buffered data and no progress is made, the channel buffered status is
                    //cleared to avoid the overhead of checking every time.
                    keysWithBufferedRead.add(key);
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                //处理写事件，写完将结果保存进completedSends中
                if (channel.ready() && key.isWritable()) {
                    Send send = null;
                    try {
                        //将send中的数据发送出去，send不等于null则说明完全发送出去了,并取消了写事件
                        // 如果write返回值为null，则说明正在发送中，下一次继续发
                        send = channel.write();
                    } catch (Exception e) {
                        sendFailed = true;
                        throw e;
                    }
                    if (send != null) {
                        //完全发送出去，则保存在完全发送的集合中completedSends
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                //isValid用于检测key是否有效，key创建之后就是有效的，
                // 当取消后、channel关闭、它的selector 关闭了就是无效的
                //如果key已经失效，则优雅的关闭channel
                if (!key.isValid())
                    close(channel, CloseMode.GRACEFUL);

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException)
                    log.debug("Connection with {} disconnected", desc, e);
                else if (e instanceof AuthenticationException) // will be logged later as error by clients
                    log.debug("Connection with {} disconnected due to authentication exception", desc, e);
                else
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                //抛出异常，则认为连接关闭，处理写事件时发生异常，NOTIFY_ONLY，这个状态时会将连接保存到disconnected集合中，会发送重连
                close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
            } finally {
                //记录连接的执行时间
                maybeRecordTimePerConnection(channel, channelStartTimeNanos);
            }
        }
    }

    //如果内存比较小的时候，即可用内存 < memoryPool*0.1,那么这个时候可能总是一个读事件在处理，造成其他事件饥饿，因此shuffle一下，如果内存够的话不会这样操作
    private Collection<SelectionKey> determineHandlingOrder(Set<SelectionKey> selectionKeys) {
        //it is possible that the iteration order over selectionKeys is the same every invocation.
        //this may cause starvation of reads when memory is low. to address this we shuffle the keys if memory is low.
        if (!outOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
            List<SelectionKey> shuffledKeys = new ArrayList<>(selectionKeys);
            Collections.shuffle(shuffledKeys);
            return shuffledKeys;
        } else {
            return selectionKeys;
        }
    }

    //处理读事件
    //读取transportlayer的数据到NetworkReceive对象中，然后保存到stagedReceives中
    // 如果stagedReceives中有该channel的数据，则不进行读取数据，直接返回了，在poll方法的最后面会从stagedReceives中取出第一个response保存到completeReceives中
    //这样的意思是，读取消息时必须读满一个NetworkReceive，然后保存在stagedReceives中，
    private void attemptRead(SelectionKey key, KafkaChannel channel) throws IOException {
        //if channel is ready and has bytes to read from socket or buffer, and has no
        //previous receive(s) already staged or otherwise in progress then read from it
        //channel.ready() 表示要处于ready状态才行，明文一直是true
        //key.isReadable()表示读事件；hasBytesBuffered该方法明文一直是false
        // !hasStagedReceive(channel)为true表示，stagedReceives中还未保存该KafkaChannel的数据
        if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasStagedReceive(channel) && !explicitlyMutedChannels.contains(channel)) {
            NetworkReceive networkReceive;
            //从while的条件可以看出，networkReceive非空则执行后续操作，非空的时候说明networkReceive已经读满了
            //如果为空，则一直循环，只能读满，该while才能退出
            while ((networkReceive = channel.read()) != null) {
                madeReadProgressLastPoll = true;
                addToStagedReceives(channel, networkReceive);
            }
            if (channel.isMute()) {
                outOfMemory = true; //channel has muted itself due to memory pressure.
            } else {
                madeReadProgressLastPoll = true;
            }
        }
    }

    // Record time spent in pollSelectionKeys for channel (moved into a method to keep checkstyle happy)
    private void maybeRecordTimePerConnection(KafkaChannel channel, long startTimeNanos) {
        if (recordTimePerConnection)
            channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos);
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
        explicitlyMutedChannels.add(channel);
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        // Remove the channel from explicitlyMutedChannels only if the channel has been actually unmuted.
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close the idle connection from {} due to being idle for {} millis",
                            connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                channel.state(ChannelState.EXPIRED);
                close(channel, CloseMode.GRACEFUL);
            }
        }
    }

    /**
     * Clear the results from the prior poll
     */
    //清空之前poll的数据
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        // Remove closed channels after all their staged receives have been processed or if a send was requested
        //如果closingChannels非空，循环closingChannels，
        // 如果stagedReceives中该nodeid的数据已经处理完了，
        // 如果failedSends中有该nodeid的值，则说明在closing的时候有请求send数据，那么删除failedSends的nodeid，
        // 这两种情况只要一个为true,则调用doClose(channel, true);关闭
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            Deque<NetworkReceive> deque = this.stagedReceives.get(channel);
            boolean sendFailed = failedSends.remove(channel.id());
            if (deque == null || deque.isEmpty() || sendFailed) {
                doClose(channel, true);
                it.remove();
            }
        }
        //如果没有closingChannels，那么就是send的时候有正在发送的数据，出现的异常
        //failedSends记录在那些nodeid上发送失败了，将这些有发送失败的nodeid，记录在disconnected中，
        // disconnected会在networkclient的poll的handler*方法中，将请求
        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        this.madeReadProgressLastPoll = false;
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param timeoutMs Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     */
    //返回值表示有多少个通道就绪了
    private int select(long timeoutMs) throws IOException {
        if (timeoutMs < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (timeoutMs == 0L)
            //非阻塞 不管什么通道，直接返回，如果没有通道准备就返回0
            return this.nioSelector.selectNow();
        else
            //select()阻塞到至少有一个通道在你注册的事件上就绪了  select(timeout)和select()一样，只是多了超时时间
            return this.nioSelector.select(timeoutMs);
    }

    /**
     * Close the connection identified by the given id
     */
    //networkclient的超时处理handleTimedOutRequests,会调用该方法
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        //如果是正常channel，则设置channel状态为LOCAL_CLOSE，调用close(channel, false);
        if (channel != null) {
            // There is no disconnect notification for local close, but updating
            // channel state here anyway to avoid confusion.
            channel.state(ChannelState.LOCAL_CLOSE);
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
        } else {
            //如果nodeid在closingChannels中，则调用doClose(closingChannel, false);
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            // Close any closing channel, leave the channel in the state in which closing was triggered
            if (closingChannel != null)
                doClose(closingChannel, false);
        }
    }

    /**
     * Begin closing this connection.
     * If 'closeMode' is `CloseMode.GRACEFUL`, the channel is disconnected here, but staged receives
     * are processed. The channel is closed when there are no outstanding receives or if a send is
     * requested. For other values of `closeMode`, outstanding receives are discarded and the channel
     * is closed immediately.
     *
     * The channel will be added to disconnect list when it is actually closed if `closeMode.notifyDisconnect`
     * is true.
     */
    //如果“closeMode”是“CloseMode.GRACEFUL”，则通道被断开，但staged receives被处理。当没有未完成的接收或请求发送时，信道被关闭。
    // 对于“关闭模式”的其他值，未完成的接收被丢弃，并且信道立即关闭。
    // 如果closeMode.notifyDisconnect=true,则会真正关闭连接，并保存到 disconnect 列表中
    // 那么经过该方法，connected中移除，channels中移除，取消了channel，其实就是关闭了连接同时清除了连接的缓存
    private void close(KafkaChannel channel, CloseMode closeMode) {
        //设置KafkaChannel的disconnected=true，取消了channel
        channel.disconnect();

        // Ensure that `connected` does not have closed channels. This could happen if `prepare` throws an exception
        // in the `poll` invocation when `finishConnect` succeeds
        //connected集合中异常该nodeid
        connected.remove(channel.id());

        // Keep track of closed channels with pending receives so that all received records
        // may be processed. For example, when producer with acks=0 sends some records and
        // closes its connections, a single poll() in the broker may receive records and
        // handle close(). When the remote end closes its connection, the channel is retained until
        // a send fails or all outstanding receives are processed. Mute state of disconnected channels
        // are tracked to ensure that requests are processed one-by-one by the broker to preserve ordering.
        Deque<NetworkReceive> deque = this.stagedReceives.get(channel);
        //正确关闭的连接会保存到closingChannels集合中
        // 如果是正常关闭，并且读取到的请求还没处理完，则保存到closingChannels这个集合中
        // closingChannels里面保存的channel其实已经取消了，hasStagedReceives()有数据的话，在poll中timeout=0，
        // 然后select直接返回，如果没有事件，就会转移stagedReceives到completedReceives，
        // 这个closingChannels的作用，是在该类send方法是时候，如果nodeid在closingChannels，那么就把nodeid保存在了failedSends中，
        // 而failedSends中的数据最终会保存进 disconnected中
        if (closeMode == CloseMode.GRACEFUL && deque != null && !deque.isEmpty()) {
            // stagedReceives will be moved to completedReceives later along with receives from other channels
            closingChannels.put(channel.id(), channel);
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id());
        } else
            doClose(channel, closeMode.notifyDisconnect);
        //close的时候移除channels里的KafkaChannel
        this.channels.remove(channel.id());

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }

    //只有在notifyDisconnect=true的情况下，才把channel保存在disconnected集合中
    // 在send方法中，异常时是，CloseMode.DISCARD_NO_NOTIFY，即没有保存在disconnected集合中
    private void doClose(KafkaChannel channel, boolean notifyDisconnect) {
        SelectionKey key = channel.selectionKey();
        try {
            immediatelyConnectedKeys.remove(key);
            keysWithBufferedRead.remove(key);
            //关闭各种资源
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        } finally {
            //通过key.cancel来关闭channel
            key.cancel();
            key.attach(null);
        }
        this.sensors.connectionClosed.record();
        //移除还未处理的response
        this.stagedReceives.remove(channel);
        this.explicitlyMutedChannels.remove(channel);
        if (notifyDisconnect)
            this.disconnected.put(channel.id(), channel.state());
    }

    /**
     * check if channel is ready
     */
    //确认KafkaChannel是否是ready状态,如果该node还没有建立连接，则channel为null，方法返回false
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel openOrClosingChannelOrFail(String id) {
        //channels集合中保存的是该node上的连接，通过nodeid从集合中获取chanel
        KafkaChannel channel = this.channels.get(id);
        //如果channels中没有，就看看关闭的集合中有木有closingChannels
        if (channel == null)
            channel = this.closingChannels.get(id);
        //如果还是木有，就抛异常
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Return the channel with the specified id if it was disconnected, but not yet closed
     * since there are outstanding messages to be processed.
     */
    public KafkaChannel closingChannel(String id) {
        return closingChannels.get(id);
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    /**
     * check if stagedReceives have unmuted channel
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute())
                return true;
        }
        return false;
    }


    /**
     * adds a receive to staged receives
     */
    //将NetworkReceive 保存到 stagedReceives 中
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        //将NetworkReceive 保存到 stagedReceives 中
        if (!stagedReceives.containsKey(channel))
            stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);//添加到队列的队尾
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     */
    //检查是否有staged receives，有的话添加到 completedReceives
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!explicitlyMutedChannels.contains(channel)) {
                    Deque<NetworkReceive> deque = entry.getValue();
                    addToCompletedReceives(channel, deque);
                    if (deque.isEmpty())
                        iter.remove();
                }
            }
        }
    }

    //将stagedDeque队列的第一个元素添加到completedReceives中，并在stagedDeque中删除该元素
    private void addToCompletedReceives(KafkaChannel channel, Deque<NetworkReceive> stagedDeque) {
        NetworkReceive networkReceive = stagedDeque.poll();//删除并返回deque的第一个元素
        this.completedReceives.add(networkReceive);//添加到已经完成的请求集合中
        this.sensors.recordBytesReceived(channel.id(), networkReceive.size());
    }

    // only for testing
    public Set<SelectionKey> keys() {
        return new HashSet<>(nioSelector.keys());
    }

    // only for testing
    public int numStagedReceives(KafkaChannel channel) {
        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        return deque == null ? 0 : deque.size();
    }

    private class SelectorMetrics {
        private final Metrics metrics;
        private final String metricGrpPrefix;
        private final Map<String, String> metricTags;
        private final boolean metricsPerConnection;

        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor successfulAuthentication;
        public final Sensor failedAuthentication;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
            this.metrics = metrics;
            this.metricGrpPrefix = metricGrpPrefix;
            this.metricTags = metricTags;
            this.metricsPerConnection = metricsPerConnection;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
            this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-close", "connections closed"));

            this.connectionCreated = sensor("connections-created:" + tagsSuffix);
            this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-creation", "new connections established"));

            this.successfulAuthentication = sensor("successful-authentication:" + tagsSuffix);
            this.successfulAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-authentication", "connections with successful authentication"));

            this.failedAuthentication = sensor("failed-authentication:" + tagsSuffix);
            this.failedAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-authentication", "connections with failed authentication"));

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix);
            bytesTransferred.add(createMeter(metrics, metricGrpName, metricTags, new Count(),
                    "network-io", "network operations (reads or writes) on all connections"));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix, bytesTransferred);
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags,
                    "outgoing-byte", "outgoing bytes sent to all servers"));
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags, new Count(),
                    "request", "requests sent"));
            MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix, bytesTransferred);
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    "incoming-byte", "bytes read off all sockets"));
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    new Count(), "response", "responses received"));

            this.selectTime = sensor("select-time:" + tagsSuffix);
            this.selectTime.add(createMeter(metrics, metricGrpName, metricTags,
                    new Count(), "select", "times the I/O layer checked for new I/O to perform"));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            this.selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"));

            this.ioTime = sensor("io-time:" + tagsSuffix);
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            this.ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return channels.size();
                }
            });
        }

        private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName), metricTags);
            if (stat == null)
                return new Meter(rateMetricName, totalMetricName);
            else
                return new Meter(stat, rateMetricName, totalMetricName);
        }

        private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String descriptiveName) {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
        }

        private Meter createIOThreadRatioMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                    String.format("The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "time-total", groupName,
                    String.format("The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    nodeRequest.add(createMeter(metrics, metricGrpName, tags, "outgoing-byte", "outgoing bytes"));
                    nodeRequest.add(createMeter(metrics, metricGrpName, tags, new Count(), "request", "requests sent"));
                    MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    nodeResponse.add(createMeter(metrics, metricGrpName, tags, "incoming-byte", "incoming bytes"));
                    nodeResponse.add(createMeter(metrics, metricGrpName, tags, new Count(), "response", "responses received"));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing
    //
    private static class IdleExpiryManager {
        //用来记录各个连接的使用情况，并据此关闭空闲时间超过connectionsMaxIdleNanos的连接
        private final Map<String, Long> lruConnections;
        //连接的最长时间
        private final long connectionsMaxIdleNanos;
        private long nextIdleCloseCheckTime;

        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }

        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

    //package-private for testing
    boolean isOutOfMemory() {
        return outOfMemory;
    }

    //package-private for testing
    boolean isMadeReadProgressLastPoll() {
        return madeReadProgressLastPoll;
    }
}
