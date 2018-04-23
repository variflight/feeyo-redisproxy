package com.feeyo.redis.net.backend.pool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Node;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.kafka.admin.KafkaAdmin;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.google.common.collect.Sets;

public class KafkaPool extends AbstractPool {

	private Map<Integer, PhysicalNode> physicalNodes = new HashMap<Integer, PhysicalNode>(3);
	public volatile int heartbeatStatus = 1;
	/**
	 * available node list
	 */
	private Set<String> availableHostList = Sets.newConcurrentHashSet(); // new LinkedHashSet<String>();
	private Set<String> backupHostList = Sets.newConcurrentHashSet(); // 极端情况下的一种防护措施

	public KafkaPool(PoolCfg poolCfg) {
		super(poolCfg);
		// 初始化有效的节点列表
		this.availableHostList.addAll(poolCfg.getNodes());
		this.backupHostList.addAll(poolCfg.getNodes());
	}

	@Override
	public boolean startup() {
		Set<String> theHostList = availableHostList;
		if (theHostList.isEmpty()) {
			theHostList = backupHostList;
		}
		
		for (String availableHost : theHostList) {
			
			String[] hostAndPort = availableHost.split(":");
			KafkaAdmin kafkaAdmin = new KafkaAdmin(hostAndPort[0] + ":" + hostAndPort[1]);
			
			try {
				Collection<Node> nodeData = kafkaAdmin.getClusterNodes();
				if (nodeData == null || nodeData.isEmpty()) {
					continue;
				}
				
				int poolType = poolCfg.getType();
				String poolName = poolCfg.getName();
				int minCon = poolCfg.getMinCon();
				int maxCon = poolCfg.getMaxCon();
				final RedisBackendConnectionFactory bcFactory = RedisEngineCtx.INSTANCE().getBackendRedisConFactory();
				
				availableHostList.clear();
				backupHostList.clear();
				for (Node node : nodeData) {
					PhysicalNode physicalNode = new PhysicalNode(bcFactory, poolType, poolName, minCon, maxCon, node.host(), node.port() );
					physicalNode.initConnections();
					physicalNodes.put(node.id(), physicalNode);
					
					// 防止配置文件未更新的情况
					availableHostList.add(node.host() + ":" + node.port());
					backupHostList.add(node.host() + ":" + node.host());
				}
				// 只需一个node 查询成功, 即跳出
				break;
				
			} catch (Exception e) {
				LOGGER.error("discover kafka pool err:", e);
			} finally {
				if (kafkaAdmin != null) {
					kafkaAdmin.close();
				}
			}
		}
		return true;
	}

	@Override
	public boolean close(boolean isForce) {
		for (PhysicalNode physicalNode : physicalNodes.values()) {
			physicalNode.clearConnections("manual reload", isForce);
		}
		return true;
	}

	@Override
	public PhysicalNode getPhysicalNode() {
		return null;
	}

	@Override
	public PhysicalNode getPhysicalNode(String cmd, String key) {
		return null;
	}

	@Override
	public PhysicalNode getPhysicalNode(String cmd, byte[] key) {
		return null;
	}

	@Override
	public boolean testConnection() {
		// TODO 后期添加
		return false;
	}

	@Override
	public void availableCheck() {
		// 后期添加

	}

	@Override
	public void heartbeatCheck(long timeout) {
		// TODO 后期添加

	}

	@Override
	public PhysicalNode getPhysicalNode(int id) {
		return physicalNodes.get(id);
	}

}
