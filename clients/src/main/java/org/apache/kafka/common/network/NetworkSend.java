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

import java.nio.ByteBuffer;

/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        super(destination, sizeDelimit(buffer));
    }

    //该方法创建了了一个ByteBuffer数组，一个保存size的buffer，一个保存消息的buffer
    private static ByteBuffer[] sizeDelimit(ByteBuffer buffer) {
        //serialize方法的时候，rewind方法将position=0，因此 buffer.remaining()应该是buffer的容量
        return new ByteBuffer[] {sizeBuffer(buffer.remaining()), buffer};
    }

    //创建一个4字节的sizeBuffer,用于保存报文的大小
    private static ByteBuffer sizeBuffer(int size) {
        //创建4个字节的ByteBuffer
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        //将消息的剩余大小写入sizeBuffer
        sizeBuffer.putInt(size);
        //position=0
        sizeBuffer.rewind();
        return sizeBuffer;
    }

}
