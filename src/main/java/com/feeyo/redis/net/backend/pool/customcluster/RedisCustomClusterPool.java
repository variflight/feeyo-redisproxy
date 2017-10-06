package com.feeyo.redis.net.backend.pool.customcluster;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.customcluster.rule.RuleAlgorithm;
import com.feeyo.redis.net.backend.pool.customcluster.rule.RuleAlgorithmFactory;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * custom cluster pool
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RedisCustomClusterPool extends AbstractPool {
    private Map<String/* connection string ex: 127.0.0.1:8888 */, CustomClusterNode> nodes = new HashMap<>();
    private RuleAlgorithm ruleAlgorithm;

    public RedisCustomClusterPool(PoolCfg poolCfg) {
        super(poolCfg);
    }

    @Override
    public boolean startup() {
        ruleAlgorithm = RuleAlgorithmFactory.getRuleAlgorithm(poolCfg.getRule());

        int type = poolCfg.getType();
        String name = poolCfg.getName();
        int minCon = poolCfg.getMinCon();
        int maxCon = poolCfg.getMaxCon();

        for (String nodeConnStr : poolCfg.getNodes()) {
            CustomClusterNode ccNode = new CustomClusterNode();
            String[] ipAndPort = nodeConnStr.split(":");
            String ip = ipAndPort[0];
            int port = Integer.parseInt(ipAndPort[1]);

            RedisBackendConnectionFactory factory = RedisEngineCtx.INSTANCE().getBackendRedisConFactory();
            PhysicalNode physicalNode = new PhysicalNode(factory, type, name, minCon, maxCon, ip, port);
            physicalNode.initConnections();
            ccNode.setPhysicalNode(physicalNode);
            nodes.put(nodeConnStr, ccNode);
        }
        return false;
    }

    @Override
    public boolean close(boolean isForce) {
        return false;
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public PhysicalNode getPhysicalNode(String cmd, String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public PhysicalNode getPhysicalNode(String cmd, byte[] key) {
        return getPhysicalNode(cmd, new String(key));
    }

    @Override
    public boolean testConnection() {
        for (Map.Entry<String, CustomClusterNode> entry : nodes.entrySet()) {
            PhysicalNode physicalNode = entry.getValue().getPhysicalNode();
            String host = physicalNode.getHost();
            int port = physicalNode.getPort();

            JedisConnection conn = null;
            try {
                conn = new JedisConnection(host, port, 2000, 0);
                conn.sendCommand(RedisCommand.PING);
                String ret = conn.getBulkReply();
                if (!ret.toUpperCase().contains("PONG")) {
                    return false;
                }
            } catch (JedisConnectionException e) {
                LOGGER.error("test connection err: {}:{}", host, port);
                return false;
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        }
        return true;
    }

    @Override
    public void availableCheck() {
        if ( !availableCheckFlag.compareAndSet(false,  true) ) {
            return;
        }

        try {
            for (Map.Entry<String, CustomClusterNode> entry : nodes.entrySet()) {
                CustomClusterNode ccNode = entry.getValue();
                ccNode.availableCheck();
            }
        } finally {
            availableCheckFlag.set( false );
        }
    }

    @Override
    public void heartbeatCheck(long timeout) {
    }

    public PhysicalNode getPhysicalNode(RedisRequest request) throws PhysicalNodeUnavailableException {
        int idx = ruleAlgorithm.calculate(request);
        // 暂不对 pool.xml 添加更多信息，以加载顺序作为索引顺序
        if (idx > poolCfg.getNodes().size()) {
            throw new PhysicalNodeUnavailableException("custom cluster pool node unavailable: " + idx);
        }

        String str = poolCfg.getNodes().get(idx);
        CustomClusterNode node = nodes.get(str);
        return node.getPhysicalNode();
    }
}
