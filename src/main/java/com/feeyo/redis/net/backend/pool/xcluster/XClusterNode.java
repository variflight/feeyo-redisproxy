package com.feeyo.redis.net.backend.pool.xcluster;

import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * Custom cluster node
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class XClusterNode {
    private PhysicalNode physicalNode = null;
//    private PhysicalNode phyNodeFollow = null;

    private volatile int heartbeatRetry = 0;
    private volatile int heartbeatStatus = 1;
    private volatile long heartbeatTime = -1;

    public PhysicalNode getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(PhysicalNode physicalNode) {
        this.physicalNode = physicalNode;
    }

    public void availableCheck() {
        String host = physicalNode.getHost();
        int port = physicalNode.getPort();

        JedisConnection conn = null;
        try {
            conn = new JedisConnection(host, port, 2000, 0);
            conn.sendCommand(RedisCommand.PING);
            String ret = conn.getBulkReply();
            if (ret.toUpperCase().contains("PONG")) {
                heartbeatRetry = 0;
                heartbeatStatus  = 1;
            } else {
                if ( ++heartbeatRetry == 3 ) {
                    heartbeatStatus = -1;
                }
            }
        } catch (JedisConnectionException e) {
            if ( ++heartbeatRetry == 3 ) {
                heartbeatStatus = -1;
            }
        } finally {
            heartbeatTime = TimeUtil.currentTimeMillis();
            if ( conn != null ) {
                conn.disconnect();
            }
        }

        if ( heartbeatStatus == -1 ) {
            physicalNode.clearConnections("this node exception, automatic reload", true);
        }
    }
}
