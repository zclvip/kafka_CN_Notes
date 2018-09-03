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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private final Logger log;

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    //每个node的connection的状态管理
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    //在途的请求，即已经发送但是未响应的请求集合
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* default timeout for individual requests to await acknowledgement from servers */
    //request的默认超时时间
    private final int defaultRequestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server */
    private final long reconnectBackoffMs;

    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private final boolean discoverBrokerVersions;

    private final ApiVersions apiVersions;

    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    private final Sensor throttleTimeSensor;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    public NetworkClient(Selectable selector,
            Metadata metadata,
            String clientId,
            int maxInFlightRequestsPerConnection,
            long reconnectBackoffMs,
            long reconnectBackoffMax,
            int socketSendBuffer,
            int socketReceiveBuffer,
            int defaultRequestTimeoutMs,
            Time time,
            boolean discoverBrokerVersions,
            ApiVersions apiVersions,
            Sensor throttleTimeSensor,
            LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          long reconnectBackoffMax,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int defaultRequestTimeoutMs,
                          Time time,
                          boolean discoverBrokerVersions,
                          ApiVersions apiVersions,
                          Sensor throttleTimeSensor,
                          LogContext logContext) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    //检查node的连接是否OK，返回true，则说明可以发送数据
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        //检查是否可以向node发送数据，如果可以直接返回了
        if (isReady(node, now))
            return true;

        //如果 isReady 为false，则有两种情况可以重建连接
        // 1、还未建立连接
        // 2、连接是 DISCONNECTED 状态,且已经过了重试的退避时间
        // hander*的方法会更新connectionStates的各种状态，目的就是根据这些状态查看连接是否ok，对外提供服务
        // DISCONNECTED这种状态会重新初始化连接
        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            //初始化连接
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        if (connectionStates.isDisconnected(nodeId))
            return;

        selector.close(nodeId);
        List<ApiKeys> requestTypes = new ArrayList<>();
        long now = time.milliseconds();
        for (InFlightRequest request : inFlightRequests.clearAll(nodeId)) {
            if (request.isInternalRequest) {
                if (request.header.apiKey() == ApiKeys.METADATA) {
                    metadataUpdater.handleDisconnection(request.destination);
                }
            } else {
                requestTypes.add(request.header.apiKey());
                abortedSends.add(new ClientResponse(request.header,
                        request.callback, request.destination, request.createdTimeMs, now,
                        true, null, null, null));
            }
        }
        //关闭连接
        connectionStates.disconnected(nodeId, now);
        if (log.isDebugEnabled()) {
            log.debug("Manually disconnected from {}. Removed requests: {}.", nodeId,
                Utils.join(requestTypes, ", "));
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (InFlightRequest request : inFlightRequests.clearAll(nodeId))
            if (request.isInternalRequest && request.header.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleDisconnection(request.destination);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * @param node the connection to check
     * @param now the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    //判断是否可以向node发送请求，主要是符合三个条件
    //1、metadata并未处于正在更新或者需要更新的状态
    // 2、已经成功建立建立且连接正常 即 连接状态是ready
    // 3、通过在途数据判断可以发送数据
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        //如果metadataUpdater.isUpdateDue(now) 为true,则表示需要更新metadata数据了，这时候应该首先更新 metadata request
        //所有node ready 必须metadata已经准备好了，即 metadataUpdater.isUpdateDue(now) 为false
        //canSendRequest(node.idString(), now) 为true，表示和node的连接状态 良好
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    //判断是否可以向node发送消息 主要从
    // 1、连接状态是否是ready状态、
    // 2、selector的KafkaChannel是否是ready状态 即检查网络协议正常，且通过了身份验证
    // 3、通过在途请求判断是否可以发送数据
    // 通过三个维度确认是否可以发送
    private boolean canSendRequest(String node, long now) {
        //connectionStates.isReady(node, now) 为true，表示当前连接是ready状态，并且不是处于节流时间段内
        //selector.isChannelReady(node) 是确认KafkaChannel是否是ready状态，明文实现一直是true
        //inFlightRequests.canSendMore(node) 判断是否可以向node发送数据，主要是根据在途请求来判断
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
            inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    //发送 ClientRequest 请求
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    //发送内部的元数据请求 注意内部请求，isInternalRequest为true
    private void sendInternalMetadataRequest(MetadataRequest.Builder builder,
                                             String nodeConnectionId, long now) {
        //组装ClientRequest对象
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        //调用doSend方法发送请求
        doSend(clientRequest, true, now);
    }

    //isInternalRequest为true时，表示是内部请求，那么在发送时不需调用 canSendRequest 校验能否发送，因为内部请求在发送之前已经做了校验
    // 为false时，为外部请求，必须要校验
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        //node节点的id
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }

        //获取ClientRequest的builder
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);
            //如果异常了，就保存进废弃的response集合中
            abortedSends.add(clientResponse);
        }
    }

    //上面两个参数的方法，实际进来一些校验和异常处理，实际是调用了这个方法进行发送数据
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        //获取nodeid
        String destination = clientRequest.destination();
        //创建requestHeader对象
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            int latestClientVersion = clientRequest.apiKey().latestVersion();
            if (header.apiVersion() == latestClientVersion) {
                log.trace("Sending {} {} with correlation id {} to node {}", clientRequest.apiKey(), request,
                        clientRequest.correlationId(), destination);
            } else {
                log.debug("Using older server API v{} to send {} {} with correlation id {} to node {}",
                        header.apiVersion(), clientRequest.apiKey(), request, clientRequest.correlationId(), destination);
            }
        }
        //调用request的toSend方法将消息保存到send中
        Send send = request.toSend(destination, header);
        //封装在途对象，并保存进在途对象集合中
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);

        //调用selector的send方法进行发送，其实是将send的消息发送到KafkaChannel的send中
        selector.send(send);
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    //真正执行网络i/o的地方
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        //1、首先看看是否有废弃的请求，废弃是因为版本不支持而发生的异常或者disconnects，doSend两个参数的方法异常处理时会保存数据
        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.

            //处理逻辑是清空abortedSends，执行callback方法，并返回abortedSends
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        //2 、更新metadata数据
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            //3、执行i/o操作
            // poll方法的处理结果基本都放在了那四个集合中，后面的handle*方法处理的是集合中的数据
            this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        //处理completedSends队列
        handleCompletedSends(responses, updatedNow);
        //处理completedReceive队列
        handleCompletedReceives(responses, updatedNow);
        //处理disconnected队列
        handleDisconnections(responses, updatedNow);
        //处理connected队列
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        //处理inFlightRequests中超时的请求
        handleTimedOutRequests(responses, updatedNow);
        //循环执行callback
        completeResponses(responses);

        return responses;
    }

    //该方法是对response的处理，循环处理每一个response
    // 如果有callback方法，则执行callback
    private void completeResponses(List<ClientResponse> responses) {
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
        this.metadataUpdater.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     *
     * @return The node with the fewest in-flight requests.
     */
    //寻找负载最小的node
    //寻找在途请求最小的node，优先选择有连接的node，但是也会选择一个还没有连接的node
    //但是永远不会选择一个还在重连的backOff时间内的断开的连接
    @Override
    public Node leastLoadedNode(long now) {
        //从元数据中获取nodes集合
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;

        //取一个随机值，防止饥饿
        int offset = this.randOffset.nextInt(nodes.size());
        //循环nodes
        for (int i = 0; i < nodes.size(); i++) {
            //计算索引
            int idx = (offset + i) % nodes.size();
            //获取node
            Node node = nodes.get(idx);
            //根据nodeid，获取该node在途的消息数量
            int currInflight = this.inFlightRequests.count(node.idString());
            //如果木有在途消息，且和node的连接通信良好，则可以向该节点发送请求，并且立即返回，选定了该节点
            if (currInflight == 0 && isReady(node, now)) {
                // if we find an established connection with no in-flight requests we can stop right away
                log.trace("Found least loaded node {} connected with no in-flight requests", node);
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                //这里排除了不能重连的情况，则选择在途请求最小的node节点
                //this.connectionStates.isBlackedOut(node.idString(), now) 返回false的情况有三种
                //有一种情况是还没有建立连接，这个node也是可选的，
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            } else if (log.isTraceEnabled()) {
                log.trace("Removing node {} from least loaded node selection: is-blacked-out: {}, in-flight-requests: {}",
                        node, this.connectionStates.isBlackedOut(node.idString(), now), currInflight);
            }
        }

        if (found != null)
            log.trace("Found least loaded node {}", found);
        else
            log.trace("Least loaded node selection failed to find an available node");

        return found;
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(responseBuffer, requestHeader, null, 0);
        return AbstractResponse.parseResponse(requestHeader.apiKey(), responseStruct);
    }

    private static Struct parseStructMaybeUpdateThrottleTimeMetrics(ByteBuffer responseBuffer, RequestHeader requestHeader,
                                                                    Sensor throttleTimeSensor, long now) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        Struct responseBody = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), responseBuffer);
        correlate(requestHeader, responseHeader);
        if (throttleTimeSensor != null && responseBody.hasField(CommonFields.THROTTLE_TIME_MS))
            throttleTimeSensor.record(responseBody.get(CommonFields.THROTTLE_TIME_MS), now);
        return responseBody;
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        //connectionStates是在Networkclient层面维护的连接状态信息，
        // 先将连接状态设置为DISCONNECTED状态
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        //根据ChannelState的状态进程处理各种情况
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                //如果是认证失败，则将connectionStates状态设置为AUTHENTICATION_FAILED
                connectionStates.authenticationFailed(nodeId, now, exception);
                metadataUpdater.handleAuthenticationFailure(exception);
                log.error("Connection to node {} failed authentication due to: {}", nodeId, exception.getMessage());
                break;
            case AUTHENTICATE:
                // This warning applies to older brokers which don't provide feedback on authentication failures
                log.warn("Connection to node {} terminated during authentication. This may indicate " +
                        "that authentication failed due to invalid credentials.", nodeId);
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} could not be established. Broker may not be available.", nodeId);
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }
        //如果是Disconnection这种情况，将该node在途请求队列删除，这些在途请求队列的请求需要重新发送，
        for (InFlightRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} {} with correlation id {} due to node {} being disconnected",
                    request.header.apiKey(), request.request, request.header.correlationId(), nodeId);
            if (!request.isInternalRequest)
                //如果不是内部请求，则封装ClientResponse对象，disconnected=true,带着异常信息
                responses.add(request.disconnected(now, disconnectState.exception()));
            else if (request.header.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleDisconnection(request.destination);
        }
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    //处理超时的请求
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        //判断在途请求中，那些nodeid已经超时了
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        //循环处理超时的每个nodeid
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }

        // we disconnected, so we should probably refresh our metadata
        if (!nodeIds.isEmpty())
            metadataUpdater.requestUpdate();
    }

    //清空了abortedSends
    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destination());
            //检查请求是否需要响应
            if (!request.expectResponse) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    //处理从服务端读取的数据，封装成了ClientResponse对象，保存在response集合中
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            //取出该node的在途队列最早的请求，在途保存请求的时候是一直往对头加，即最早的消息在队尾
            // completedReceives按数组保存也是有序的最早的消息保存在前面
            // 这样completedReceives的第一个response和在途队列的最后一条request是对应的
            // 注意，在途队列在执行completeNext方法获取在途请求的时候，就已经删除了该请求，
            // 所有在在途请求的生命周期是，在networkClient发送clientResponse之前保存进行，具体的方法在三个参数的doSend中
            // 在收到请求响应的时候，从在途集合中进行了删除，并用该在途对象创建了ClientResponse对象
            InFlightRequest req = inFlightRequests.completeNext(source);
            Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(receive.payload(), req.header,
                throttleTimeSensor, now);
            if (log.isTraceEnabled()) {
                log.trace("Completed receive from node {} for {} with correlation id {}, received {}", req.destination,
                    req.header.apiKey(), req.header.correlationId(), responseStruct);
            }
            // If the received response includes a throttle delay, throttle the connection.
            AbstractResponse body = AbstractResponse.parseResponse(req.header.apiKey(), responseStruct);
            maybeThrottle(body, req.header.apiVersion(), req.destination, now);
            //metadataResponse处理
            if (req.isInternalRequest && body instanceof MetadataResponse)
                metadataUpdater.handleCompletedMetadataResponse(req.header, now, (MetadataResponse) body);
            else if (req.isInternalRequest && body instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) body);
            else
                responses.add(req.completed(body, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.error() != Errors.NONE) {
            if (req.request.version() == 0 || apiVersionsResponse.error() != Errors.UNSUPPORTED_VERSION) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        apiVersionsResponse.error(), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder((short) 0));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.apiVersions());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        log.debug("Recorded API versions for node {}: {}", node, nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        //循环处理selector的disconnected维护的nodeid的连接信息
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now, entry.getValue());
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    //处理已连接的集合，即将connectionStates设置为ready状态，标识该连接是可用的
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Node that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                this.connectionStates.checkingApiVersions(node);
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
    }

    /**
     * Initiate a connection to the given node
     */
    //为给node初始化一个连接
    //1、首先设置连接状态为 CONNECTING
    // 2、调用selector.connect方法发起连接
    //之后再调用selector.pollSelectionKeys()方法时，判断连接是否建立，如果建立，状态设置为 connected 状态
    private void initiateConnect(Node node, long now) {
        //nodeid即为连接id
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {}", node);
            //维护连接的 connecting 状态
            this.connectionStates.connecting(nodeConnectionId, now);
            //调用 kselector创建连接
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            //在创建连接时，只要出现了异常，就设置 disconnected状态
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            //也许是metadata的原因，设置needUpdate=true
            metadataUpdater.requestUpdate();
            log.warn("Error connecting to node {}", node, e);
        }
    }

    //默认实现
    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        //当前集群的元数据
        private final Metadata metadata;

        /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
        //用来标识是否已经发送了MetadataRequest请求更新Metadata，如果已经发送就没必要重复发送
        private boolean metadataFetchInProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        //如果没有正在更新metadata的请求，并且下一次需要更新等待时间已经为0，即需要更新metadata数据了  该方法为true，则表示需要更新metadata数据了
        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        //检查是否可以更新metadata
        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            //计算下一次更新还需要等待多长时间
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            //计算需要等待fetch的时间
            // 如果metadataFetchInProgress=true,表示已经发送了更新元数据的请求，那么就不能立即发送了，需要等待到该已发送的请求过期之后才能继续发送
            // 如果为false，表示还没有发送请求，即长时间没有更新了，那么可以立即发送请求
            long waitForMetadataFetch = this.metadataFetchInProgress ? defaultRequestTimeoutMs : 0;

            //在metadata元数据允许更新的时间 和 允许发送更新请求的时间 之间取一个最大值
            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);

            //如果时间还没到，就返回，不能立即更新
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            //选择负载最小的node节点来发送metadata请求
            Node node = leastLoadedNode(now);
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            return maybeUpdate(now, node);
        }

        //处理因连接断开或者其他异常导致无法获取到响应 这种情况
        @Override
        public void handleDisconnection(String destination) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destination);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            //设置false，让其可以再次发生更新请求
            metadataFetchInProgress = false;
        }

        @Override
        public void handleAuthenticationFailure(AuthenticationException exception) {
            metadataFetchInProgress = false;
            if (metadata.updateRequested())
                metadata.failedUpdate(time.milliseconds(), exception);
        }

        //处理metadata的response请求
        @Override
        public void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            //response已经回来了，设置false
            this.metadataFetchInProgress = false;
            Cluster cluster = response.cluster();

            // If any partition has leader with missing listeners, log a few for diagnosing broker configuration
            // issues. This could be a transient issue if listeners were added dynamically to brokers.
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error() == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            if (cluster.nodes().size() > 0) {
                this.metadata.update(cluster, response.unavailableTopics(), now);
            } else {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now, null);
            }
        }

        @Override
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        //查看是否有一个node处于 isConnecting状态
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            //检查是否可以向该node发送请求
            if (canSendRequest(nodeConnectionId, now)) {
                //设置已发送metadata请求
                this.metadataFetchInProgress = true;
                MetadataRequest.Builder metadataRequest;
                //是否更新所有的topic，组装metadataRequest
                if (metadata.needMetadataForAllTopics())
                    metadataRequest = MetadataRequest.Builder.allTopics();
                else
                    metadataRequest = new MetadataRequest.Builder(new ArrayList<>(metadata.topics()),
                            metadata.allowAutoTopicCreation());


                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                //将metadataRequest请求发送出去，
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                //返回默认的请求超时时间
                return defaultRequestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            //走到这里，说明现在的node都不能发送消息
            //isAnyNodeConnecting() 用于查询是否有一个node处于正在创建连接状态 即 connecting状态,如果有返回重连的backOff时间
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            //检查是否可以连接，
            // 两种情况，一种是还未建立连接，那么初始化连接很好理解
            // 另一种情况是连接已经断开了，但是只要不在节流的时间内，就重新创建连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                //如果可以连接，就初始化一个连接
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, correlation++, clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    //在途请求，即已经发送出去的请求，但是还没收到响应
    static class InFlightRequest {
        final RequestHeader header;//header
        final String destination;//nodeId
        final RequestCompletionHandler callback;//callback
        final boolean expectResponse;//是否期望有response
        final AbstractRequest request;//request的引用
        final boolean isInternalRequest; //是否是内部的request used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;//send 的时间 now -sendTimeMs > requestTimeoutMs 就是超时了
        final long createdTimeMs;//创建时间
        final long requestTimeoutMs;//请求的超时时间

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        //完整的请求，disconnected=false，responseBody是有值的
        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        //注意 disconnected这种情况创建ClientResponse的时候，responseBody为null，disconnected=true
        public ClientResponse disconnected(long timeMs, AuthenticationException authenticationException) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, authenticationException, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

}
