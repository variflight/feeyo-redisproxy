package com.feeyo.config;

import com.feeyo.redis.config.NetFlowCfg;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.config.UserCfg;

import java.util.Map;
import java.util.Properties;

public abstract class AbstractConfigLoader {
    public abstract Map<String, String> loadServerMap(String uri) throws Exception;
    public abstract Map<Integer, PoolCfg> loadPoolMap(String uri) throws Exception;
    public abstract Map<String, UserCfg> loadUserMap(Map<Integer, PoolCfg> poolMap, String uri) throws Exception;
    public abstract Map<String, NetFlowCfg> loadNetFlowMap(String uri) throws Exception;
    public abstract Properties loadMailProperties(String uri) throws Exception;
}
