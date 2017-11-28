package com.feeyo.redis.net.backend.pool.xcluster;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * custom cluster pool
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class XClusterPool extends AbstractPool{
	
	//#号中间匹配若干字母和数字
	private static Pattern pattern = Pattern.compile(".*#[a-zA-Z0-9]+#$");
	
	// connection string ex: 127.0.0.1:8888:suffix
    private Map<String, XClusterNode> nodes = new HashMap<>();
    
    private static Map<String,String> suffixs = new HashMap<String, String>();

    public XClusterPool(PoolCfg poolCfg) {
        super(poolCfg);
    }

    @Override
    public boolean startup() {
        int type = poolCfg.getType();
        String name = poolCfg.getName();
        int minCon = poolCfg.getMinCon();
        int maxCon = poolCfg.getMaxCon();

        for (String nodeConnStr : poolCfg.getNodes()) {
            XClusterNode ccNode = new XClusterNode();
            String[] ipPortSuffix = nodeConnStr.split(":");
            String ip = ipPortSuffix[0];
            int port = Integer.parseInt(ipPortSuffix[1]);
            String suffix = ipPortSuffix[2];
            suffixs.put(suffix, nodeConnStr);
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
        for (Map.Entry<String, XClusterNode> entry : nodes.entrySet()) {
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
            for (Map.Entry<String, XClusterNode> entry : nodes.entrySet()) {
                XClusterNode ccNode = entry.getValue();
                ccNode.availableCheck();
            }
        } finally {
            availableCheckFlag.set( false );
        }
    }

    @Override
    public void heartbeatCheck(long timeout) {
    }

    //改写key并且返回node
    public PhysicalNode getPhysicalNode(RedisRequest request) {
    	PhysicalNode node = null;
    	byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
        if (requestKey != null) {
            String key = new String(requestKey);
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches()) {
                String[] strs = key.split("#");
                String suffix = strs[strs.length - 1];
                request.getArgs()[1] = key.substring(0,key.length()-suffix.length()-2).getBytes();
                node =  nodes.get(suffixs.get(suffix)).getPhysicalNode();
            }
        }
        return node;
    }
}
