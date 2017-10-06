package com.feeyo.redis.net.backend.pool;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.backend.pool.customcluster.RedisCustomClusterPool;

/**
 * redis [standalone, cluster, custom cluster] pool factory
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RedisPoolFactory {
    public static AbstractPool createPoolByCfg(PoolCfg poolCfg) {
        AbstractPool pool;
        switch (poolCfg.getType()) {
            case 0:
                pool = new RedisStandalonePool( poolCfg );
                break;
            case 1:
                pool = new RedisClusterPool( poolCfg );
                break;
            case 2:
                pool = new RedisCustomClusterPool( poolCfg );
                break;
            default:
                pool = new RedisStandalonePool( poolCfg );
        }
        return pool;
    }
}
