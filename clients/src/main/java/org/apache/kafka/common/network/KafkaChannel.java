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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.Objects;

/**
 * KafkaChannel是在SocketChannel上又封装了一层，只是中间多了一个间接层，TransportLayer，为了封装普通和加密的channel
 * TransportLayer封装了SocketChannel以及SelectionKey,TransportLayer根据网络协议的不同，提供不同的子类，而kafkachannel提供统一的接口
 * Send和NetworkReceive分别表示读和写时用的缓存，底层通过ByteBuffer实现，表示一次请求的数据包
 *
 */
public class KafkaChannel {
    /**
     * Mute States for KafkaChannel:
     * <ul>
     *   <li> NOT_MUTED: Channel is not muted. This is the default state. </li>
     *   <li> MUTED: Channel is muted. Channel must be in this state to be unmuted. </li>
     *   <li> MUTED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted and SocketServer has not sent a response
     *                                    back to the client yet (acks != 0) or is currently waiting to receive a
     *                                    response from the API layer (acks == 0). </li>
     *   <li> MUTED_AND_THROTTLED: (SocketServer only) Channel is muted and throttling is in progress due to quota
     *                             violation. </li>
     *   <li> MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted, throttling is in progress,
     *                                                  and a response is currently pending. </li>
     * </ul>
     */
    public enum ChannelMuteState {
        //channel的默认状态
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    };

    /** Socket server events that will change the mute state:
     * <ul>
     *   <li> REQUEST_RECEIVED: A request has been received from the client. </li>
     *   <li> RESPONSE_SENT: A response has been sent out to the client (ack != 0) or SocketServer has heard back from
     *                       the API layer (acks = 0) </li>
     *   <li> THROTTLE_STARTED: Throttling started due to quota violation. </li>
     *   <li> THROTTLE_ENDED: Throttling ended. </li>
     * </ul>
     *
     * Valid transitions on each event are:
     * <ul>
     *   <li> REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING </li>
     *   <li> RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED </li>
     *   <li> THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING </li>
     *   <li> THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING </li>
     * </ul>
     */
    public enum ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    };

    private final String id;//nodeid
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    // Tracks accumulated network thread time. This is updated on the network thread.
    // The values are read and reset after each response is sent.
    private long networkThreadTimeNanos;//计算networkThread的时间
    private final int maxReceiveSize;
    private final MemoryPool memoryPool;
    private NetworkReceive receive;
    //一个KafkaChannel一次只能存放一个数据包，在当前send数据包没有完整发出去之前，不能存放下一个
    private Send send;
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private ChannelMuteState muteState;
    private ChannelState state;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize, MemoryPool memoryPool) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.networkThreadTimeNanos = 0L;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.disconnected = false;
        //默认为not_muted状态
        this.muteState = ChannelMuteState.NOT_MUTED;
        //默认未连接状态
        this.state = ChannelState.NOT_CONNECTED;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator, receive);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, {@link TransportLayer#handshake()} performs
     * authentication. For SASL, authentication is performed by {@link Authenticator#authenticate()}.
     */
    //进行身份验证，出现异常，状态则设置为验证失败状态AUTHENTICATION_FAILED
    public void prepare() throws AuthenticationException, IOException {
        try {
            if (!transportLayer.ready())
                transportLayer.handshake();
            if (transportLayer.ready() && !authenticator.complete())
                authenticator.authenticate();
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e);
            throw e;
        }
        if (ready())
            state = ChannelState.READY;
    }

    //设置断开连接状态，设置了disconnected=true，取消了channel
    // 关闭channel有三种办法，
    // 1、channel.close方法,内部调用了key.cancel()方法
    // 2、调用slectionKey.cancel方法来取消
    // 3、中断channel的读写所在线程引起的channel关闭。
    // key.cancel()仅仅是将key加入cancelledKeys,直到下一次select才真正处理
    public void disconnect() {
        disconnected = true;
        //调用selectionkey.cancel来关闭channel
        transportLayer.disconnect();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    //判断连接已经完成了
    public boolean finishConnect() throws IOException {
        boolean connected = transportLayer.finishConnect();
        if (connected)
            //如果已经连接成功 状态设置为ready
            state = ready() ? ChannelState.READY : ChannelState.AUTHENTICATE;
        return connected;
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    //muteState 状态设置为MUTED 如果连接未断开则取消读事件
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            //如果为断开，则取消读事件
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            //muteState设置为muted
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * Unmute the channel. The channel can be unmuted only if it is in the MUTED state. For other muted states
     * (MUTED_AND_*), this is a no-op.
     *
     * @return Whether or not the channel is in the NOT_MUTED state after the call
     */
    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMute() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    //看是否ready，对于PlaintextTransportLayer，都是true
    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    //暂存消息到send中
    public void setSend(Send send) {
        //如果send非空，不能保存下一个，防止消息覆盖
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        this.send = send;//保存消息到send中
        //注册OP_WRITE事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    //从transportlayer中读取数据到NetworkReceive中，只有该NetworkReceive读满的时候在返回该对象，否则未满返回null
    public NetworkReceive read() throws IOException {
        //初始化NetworkReceive，从这里可以看出如果receive不为null，则继续往receive中读数据,但是该方法返回值为null
        NetworkReceive result = null;
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }

        //从transportLayer中读取数据到NetworkReceive中
        receive(receive);
        //如果NetworkReceive的空间都已经读满了
        if (receive.complete()) {
            //重置buffer的position=0
            receive.payload().rewind();
            //返回读满的NetworkReceive
            result = receive;
            //NetworkReceive置空，下次读取时重新创建
            receive = null;
        } else if (receive.requiredMemoryAmountKnown() && !receive.memoryAllocated() && isInMutableState()) {
            //pool must be out of memory, mute ourselves.
            mute();
        }
        return result;
    }

    //发送数据
    //把send中的数据写入到transportLayer中，如果完全写完则返回send，send置空，如果未完成发送，则返回null
    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * Returns accumulated network thread time for this channel and resets
     * the value to zero.
     */
    public long getAndResetNetworkThreadTimeNanos() {
        long current = networkThreadTimeNanos;
        networkThreadTimeNanos = 0;
        return current;
    }

    //从transportLayer中读取数据到NetworkReceive对象中
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        //将send的数据写入到transportLayer中，本质是写入了transportLayer中的SocketChannel中，即发送出去了
        send.writeTo(transportLayer);
        //如果send中已经完全发送出去，则取消写事件
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    //明文一直是false
    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
