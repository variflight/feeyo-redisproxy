package com.feeyo.redis.net.backend.pool.customcluster.rule;

import com.feeyo.redis.engine.codec.RedisRequest;

/**
 *
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public interface RuleAlgorithm {
    int calculate(RedisRequest request);
}
