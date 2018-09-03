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

import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.errors.AuthenticationException;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 *
 */
//集群的每个node的连接状态
final class ClusterConnectionStates {
    //重连的backOff的初始时间
    private final long reconnectBackoffInitMs;
    ////重连的backOff的最大时间
    private final long reconnectBackoffMaxMs;
    private final static int RECONNECT_BACKOFF_EXP_BASE = 2;
    private final double reconnectBackoffMaxExp;
    //nodeid和连接状态的对应关系，可以看出，一个node维护一个连接
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs, long reconnectBackoffMaxMs) {
        this.reconnectBackoffInitMs = reconnectBackoffMs;
        this.reconnectBackoffMaxMs = reconnectBackoffMaxMs;
        this.reconnectBackoffMaxExp = Math.log(this.reconnectBackoffMaxMs / (double) Math.max(reconnectBackoffMs, 1)) / Math.log(RECONNECT_BACKOFF_EXP_BASE);
        this.nodeState = new HashMap<>();
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id the connection id to check
     * @param now the current time in ms
     * @return true if we can initiate a new connection
     */
    //是否可以能连接，返回true，可以初始化一个新连接
    //有两种情况返回true
    // 1、还没有连接
    // 2、连接是断开状态，其两次重试之间的时间差已经过了重试的退避时间
    public boolean canConnect(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //如果state为null，表示还没有这个连接
        if (state == null)
            return true;
        else
            //有state值，如果state是未连接状态，并且现在距离最后一次连接时间差已经过了重连的backoff时间，也可以新建连接
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs >= state.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet.
     * @param id the connection to check
     * @param now the current time in ms
     */
    //该方法返回false有以下几种情况：
    // 1、该node还木有连接  这种情况可以建立连接
    // 2、当前连接是断开状态，但是已过了重试的backOff时间 这种情况可以重试连接
    // 3、当前连接不是断开状态，但是在重试的backOff时间内   不是断开状态要么是isConnected状态，要么是CONNECTING状态，那么也是可以连接的
    //返回true的情况只有一种，即当前连接是断开状态，并且还在重试的backOff时间内，在这种情况下是不可以重新连接的
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //state为null，表示nodeState还没有维护连接状态，即该node还没有创建连接
        if (state == null)
            return false;
        else
            //如果连接是断开状态，并且看看当前时间减去上次连接时间是否小于连接重试的backOff时间
            //now - state.lastConnectAttemptMs < state.reconnectBackoffMs 表示当前时间还在backOff时间内，是不能重新连接的
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs < state.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0;
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state.isDisconnected()) {
            return Math.max(state.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Return true if a specific connection establishment is currently underway
     * @param id The id of the node to check
     */
    public boolean isConnecting(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connecting state for the given connection.
     * @param id the id of the connection
     * @param now the current time
     */
    //设置连接的正在连接状态 即connecting状态
    public void connecting(String id, long now) {
        //如果nodeState中已经有了该连接，那么设置最后连接时间为当前时间，当前连接状态设置为正在连接 即CONNECTING
        if (nodeState.containsKey(id)) {
            NodeConnectionState node = nodeState.get(id);
            node.lastConnectAttemptMs = now;
            node.state = ConnectionState.CONNECTING;
        } else {
            //如果nodeState中还没保存该连接，那么保存，该连接状态为正在连接，即CONNECTING
            nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now,
                this.reconnectBackoffInitMs));
        }
    }

    /**
     * Enter the disconnected state for the given node.
     * @param id the connection we have disconnected
     * @param now the current time
     */
    //设置连接的 断开状态 即 DISCONNECTED
    public void disconnected(String id, long now) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.DISCONNECTED;
        nodeState.lastConnectAttemptMs = now;
        updateReconnectBackoff(nodeState);
    }

    /**
     * Indicate that the connection is throttled until the specified deadline.
     * @param id the connection to be throttled
     * @param throttleUntilTimeMs the throttle deadline in milliseconds
     */
    public void throttle(String id, long throttleUntilTimeMs) {
        NodeConnectionState state = nodeState.get(id);
        // The throttle deadline should never regress.
        if (state != null && state.throttleUntilTimeMs < throttleUntilTimeMs) {
            state.throttleUntilTimeMs = throttleUntilTimeMs;
        }
    }

    /**
     * Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long throttleDelayMs(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state != null && state.throttleUntilTimeMs > now) {
            return state.throttleUntilTimeMs - now;
        } else {
            return 0;
        }
    }

    /**
     * Return the number of milliseconds to wait, based on the connection state and the throttle time, before
     * attempting to send data. If the connection has been established but being throttled, return throttle delay.
     * Otherwise, return connection delay.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long pollDelayMs(String id, long now) {
        long throttleDelayMs = throttleDelayMs(id, now);
        if (isConnected(id) && throttleDelayMs > 0) {
            return throttleDelayMs;
        } else {
            return connectionDelay(id, now);
        }
    }

    /**
     * Enter the checking_api_versions state for the given node.
     * @param id the connection identifier
     */
    public void checkingApiVersions(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS;
    }

    /**
     * Enter the ready state for the given node.
     * @param id the connection identifier
     */
    public void ready(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.READY;
        nodeState.authenticationException = null;
        resetReconnectBackoff(nodeState);
    }

    /**
     * Enter the authentication failed state for the given node.
     * @param id the connection identifier
     * @param now the current time
     * @param exception the authentication exception
     */
    public void authenticationFailed(String id, long now, AuthenticationException exception) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.authenticationException = exception;
        nodeState.state = ConnectionState.AUTHENTICATION_FAILED;
        nodeState.lastConnectAttemptMs = now;
        updateReconnectBackoff(nodeState);
    }

    /**
     * Return true if the connection is in the READY state and currently not throttled.
     *
     * @param id the connection identifier
     * @param now the current time
     */
    public boolean isReady(String id, long now) {
        return isReady(nodeState.get(id), now);
    }

    //是否状态好，即  有状态且状态是ready状态，并且当前不是节流
    //不是节流的判断是，当前时间已经过了节流的时间点
    private boolean isReady(NodeConnectionState state, long now) {
        return state != null && state.state == ConnectionState.READY && state.throttleUntilTimeMs <= now;
    }

    /**
     * Return true if there is at least one node with connection in the READY state and not throttled. Returns false
     * otherwise.
     *
     * @param now the current time
     */
    public boolean hasReadyNodes(long now) {
        for (Map.Entry<String, NodeConnectionState> entry : nodeState.entrySet()) {
            if (isReady(entry.getValue(), now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if the connection has been established
     * @param id The id of the node to check
     */
    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isConnected();
    }

    /**
     * Return true if the connection has been disconnected
     * @param id The id of the node to check
     */
    public boolean isDisconnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isDisconnected();
    }

    /**
     * Return authentication exception if an authentication error occurred
     * @param id The id of the node to check
     */
    public AuthenticationException authenticationException(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null ? state.authenticationException : null;
    }

    /**
     * Resets the failure count for a node and sets the reconnect backoff to the base
     * value configured via reconnect.backoff.ms
     *
     * @param nodeState The node state object to update
     */
    private void resetReconnectBackoff(NodeConnectionState nodeState) {
        nodeState.failedAttempts = 0;
        nodeState.reconnectBackoffMs = this.reconnectBackoffInitMs;
    }

    /**
     * Update the node reconnect backoff exponentially.
     * The delay is reconnect.backoff.ms * 2**(failures - 1) * (+/- 20% random jitter)
     * Up to a (pre-jitter) maximum of reconnect.backoff.max.ms
     *
     * @param nodeState The node state object to update
     */
    //设置连接重试backoff时间  --TODO
    private void updateReconnectBackoff(NodeConnectionState nodeState) {
        if (this.reconnectBackoffMaxMs > this.reconnectBackoffInitMs) {
            nodeState.failedAttempts += 1;
            double backoffExp = Math.min(nodeState.failedAttempts - 1, this.reconnectBackoffMaxExp);
            double backoffFactor = Math.pow(RECONNECT_BACKOFF_EXP_BASE, backoffExp);
            long reconnectBackoffMs = (long) (this.reconnectBackoffInitMs * backoffFactor);
            // Actual backoff is randomized to avoid connection storms.
            double randomFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
            nodeState.reconnectBackoffMs = (long) (randomFactor * reconnectBackoffMs);
        }
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this and `disconnected`
     * is the impact on `connectionDelay`: it will be 0 after this call whereas `reconnectBackoffMs` will be taken
     * into account after `disconnected` is called.
     *
     * @param id the connection to remove
     */
    public void remove(String id) {
        nodeState.remove(id);
    }

    /**
     * Get the state of a given connection.
     * @param id the id of the connection
     * @return the state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }

    /**
     * Get the state of a given node.
     * @param id the connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }

    /**
     * The state of our connection to a node.
     */
    private static class NodeConnectionState {

        ConnectionState state;//连接的状态
        AuthenticationException authenticationException;
        long lastConnectAttemptMs;//上次连接时间
        long failedAttempts;//失败次数
        long reconnectBackoffMs;//重连的backOff
        // Connection is being throttled if current time < throttleUntilTimeMs.
        //压制时间 如果当前时间小于throttleUntilTimeMs时间，connection就开始throttled
        long throttleUntilTimeMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt, long reconnectBackoffMs) {
            this.state = state;
            this.authenticationException = null;
            this.lastConnectAttemptMs = lastConnectAttempt;
            this.failedAttempts = 0;
            this.reconnectBackoffMs = reconnectBackoffMs;
            this.throttleUntilTimeMs = 0;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ", " + failedAttempts + ", " + throttleUntilTimeMs + ")";
        }
    }
}
