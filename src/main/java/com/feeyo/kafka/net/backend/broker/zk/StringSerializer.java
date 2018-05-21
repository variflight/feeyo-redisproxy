package com.feeyo.kafka.net.backend.broker.zk;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * 基于string的序列化方式
 */
public class StringSerializer implements ZkSerializer {

    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        try {
            return new String(bytes, "utf-8");
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

    public byte[] serialize(final Object data) throws ZkMarshallingError {
        try {
            return ((String) data).getBytes("utf-8");
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
