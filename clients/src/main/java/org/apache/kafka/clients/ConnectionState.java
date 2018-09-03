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

/**
 * The states of a node connection
 *
 * DISCONNECTED: connection has not been successfully established yet
 * CONNECTING: connection is under progress
 * CHECKING_API_VERSIONS: connection has been established and api versions check is in progress. Failure of this check will cause connection to close
 * READY: connection is ready to send requests
 * AUTHENTICATION_FAILED: connection failed due to an authentication error
 */

/**
 * DISCONNECTED: 连接不没成功建立就是disconnected状态
 * CONNECTING: 连接正在建立
 * CHECKING_API_VERSIONS: 连接已经建立但是api 版本正在校验，如果版本校验失败，连接关闭
 * READY: 连接已经准备好发送数据了
 * AUTHENTICATION_FAILED: 认证失败，认证失败和disconnected均表示，连接断开了
 */
public enum ConnectionState {
    DISCONNECTED, CONNECTING, CHECKING_API_VERSIONS, READY, AUTHENTICATION_FAILED;

    //认证失败和disconnected均表示，连接断开了
    public boolean isDisconnected() {
        return this == AUTHENTICATION_FAILED || this == DISCONNECTED;
    }

    //连接状态是 CHECKING_API_VERSIONS和ready 状态
    public boolean isConnected() {
        return this == CHECKING_API_VERSIONS || this == READY;
    }
}
