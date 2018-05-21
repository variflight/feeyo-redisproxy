package com.feeyo.kafka.net.backend.broker.zk;


import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 *  基于string的序列化方式
 */
public class ByteSerializer implements ZkSerializer {

    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }

    public byte[] serialize(final Object data) throws ZkMarshallingError {
        try {
            if (data instanceof byte[]) {
                return (byte[]) data;
            } else {
                return ((String) data).getBytes("utf-8");
            }
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
