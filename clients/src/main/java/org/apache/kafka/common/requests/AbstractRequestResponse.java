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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequestResponse {
    /**
     * Visible for testing.
     */
    public static ByteBuffer serialize(Struct headerStruct, Struct bodyStruct) {
        //根据head 和 body的大小 创建ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(headerStruct.sizeOf() + bodyStruct.sizeOf());
        //先将head写入buffer
        headerStruct.writeTo(buffer);
        //在将body写入buffer
        bodyStruct.writeTo(buffer);
        //重置position=0
        buffer.rewind();
        return buffer;
    }
}
