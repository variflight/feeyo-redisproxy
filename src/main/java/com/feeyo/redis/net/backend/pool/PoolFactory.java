package com.feeyo.redis.net.backend.pool;

import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.config.PoolCfg;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.backend.pool.xcluster.XClusterPool;

/**
 * redis [standalone, cluster, custom cluster] pool factory
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class PoolFactory {
	
    public static AbstractPool createPoolByCfg(PoolCfg poolCfg) {
        AbstractPool pool;
        switch (poolCfg.getType()) {
            case PoolType.REDIS_STANDALONE:
                pool = new RedisStandalonePool( poolCfg );
                break;
            case PoolType.REDIS_CLUSTER:
                pool = new RedisClusterPool( poolCfg );
                break;
            case PoolType.REDIS_X_CLUSTER:
                pool = new XClusterPool( poolCfg );
                break;
            case PoolType.KAFKA_CLUSTER:
	            pool = new KafkaPool( poolCfg );
	            break;
            default:
                pool = new RedisStandalonePool( poolCfg );
        }
        return pool;
    }
}
