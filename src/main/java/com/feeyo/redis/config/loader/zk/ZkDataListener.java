package com.feeyo.redis.config.loader.zk;

/**
 *
 * zookeeper data changed listener
 *
 * @author Tr!bf wangyamin@variflight.com
 *
 */
public interface ZkDataListener {
    void zkDataChanged(byte[] data);
}
