package com.feeyo.kafka.net.backend.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;

import com.feeyo.kafka.admin.KafkaAdmin;
import com.feeyo.kafka.codec.ApiVersionsResponse;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.net.backend.KafkaBackendConnectionFactory;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.net.backend.callback.KafkaCmdCallback;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.util.Utils;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.google.common.collect.Sets;

import org.apache.kafka.common.protocol.Errors;

public class KafkaPool extends AbstractPool {

	protected KafkaConHeartBeatHandler conHeartBeatHanler = new KafkaConHeartBeatHandler();
	protected KafkaBackendConnectionFactory backendConFactory = new KafkaBackendConnectionFactory();

	private Map<Integer, PhysicalNode> physicalNodes = new HashMap<Integer, PhysicalNode>(3);
	public volatile int heartbeatStatus = 1;
	public volatile long heartbeatTime = -1;
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
		Collection<Node> nodes = discoverNodes();
		if (nodes == null || nodes.isEmpty()) {
			return false;
		}

		int poolType = poolCfg.getType();
		String poolName = poolCfg.getName();
		int minCon = poolCfg.getMinCon();
		int maxCon = poolCfg.getMaxCon();

		availableHostList.clear();
		backupHostList.clear();
		for (Node node : nodes) {
			PhysicalNode physicalNode = new PhysicalNode(backendConFactory, 
					poolType, poolName, minCon, maxCon, node.host(), node.port() );
			physicalNode.initConnections();
			physicalNodes.put(node.id(), physicalNode);

			// 防止配置文件未更新的情况
			availableHostList.add(node.host() + ":" + node.port());
			backupHostList.add(node.host() + ":" + node.host());
		}
		
		// 加载 ApiVersion
		loadKafkaVersion();
		
		return true;
	}

	/**
	 * 加载kafka版本
	 */
	private void loadKafkaVersion() {
		for (Entry<Integer, PhysicalNode> entry : physicalNodes.entrySet()) {

			PhysicalNode physicalNode = entry.getValue();

			try {

				RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS.id, (short) 1,
						Thread.currentThread().getName(), Utils.getCorrelationId());

				Struct struct = requestHeader.toStruct();
				final ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(struct.sizeOf() + 4);
				buffer.putInt(struct.sizeOf());
				struct.writeTo(buffer);

				// ApiVersionCallback
				KafkaCmdCallback apiVersionCallback = new KafkaCmdCallback() {
					@Override
					public void parseResponseBody(BackendConnection conn, ByteBuffer buffer) {
						Struct response = ApiKeys.API_VERSIONS.parseResponse((short) 1, buffer);
						ApiVersionsResponse ar = new ApiVersionsResponse(response);
						if (ar.isCorrect()) {
							BrokerApiVersion.setApiVersions(ar.getApiKeyToApiVersion());
						}
					}
				};

				BackendConnection backendCon = physicalNode.getConnection(apiVersionCallback, null);
				if (backendCon == null) {
					TodoTask task = new TodoTask() {
						@Override
						public void execute(BackendConnection backendCon) throws Exception {
							backendCon.write(buffer);
						}
					};
					apiVersionCallback.addTodoTask(task);
					backendCon = physicalNode.createNewConnection(apiVersionCallback, null);

				} else {
					backendCon.write(buffer);
				}
			} catch (IOException e) {
				LOGGER.warn("", e);
			}
		}
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
		Collection<Node> nodes = discoverNodes();
		if (nodes == null || nodes.isEmpty()) {
			return false;
		}
		return true;
	}
	
	private Collection<Node> discoverNodes() {
		Set<String> theHostList = availableHostList;
		if (theHostList.isEmpty()) {
			theHostList = backupHostList;
		}
		for (String availableHost : theHostList) {
			String[] hostAndPort = availableHost.split(":");
			
			KafkaAdmin kafkaAdmin = null;
			try {
				kafkaAdmin = KafkaAdmin.create(hostAndPort[0] + ":" + hostAndPort[1]);
				Collection<Node> nodes = kafkaAdmin.getClusterNodes();
				if (nodes == null || nodes.isEmpty()) {
					continue;
				}
				return nodes;
			} finally {
				if (kafkaAdmin != null)
					kafkaAdmin.close();
			}
		}
		return null;
	}

	@Override
	public void availableCheck() {
		heartbeatTime = System.currentTimeMillis();
		
		// CAS， 避免网络不好的情况下，频繁并发的检测
		if (!availableCheckFlag.compareAndSet(false, true)) {
			return;
		}
		
		try {
			Collection<Node> nodes = discoverNodes();
			if (nodes == null || nodes.isEmpty()) {
				heartbeatStatus = -1;
				LOGGER.error("kafka pool err: heartbeatStatus={}", heartbeatStatus);
			} else {
				// 1 提取所有节点
				Map<Integer, PhysicalNode> newPhysicalNodes = new HashMap<Integer, PhysicalNode>(3);
				int poolType = poolCfg.getType();
				String poolName = poolCfg.getName();
				int minCon = poolCfg.getMinCon();
				int maxCon = poolCfg.getMaxCon();

				for (Node node : nodes) {
					PhysicalNode physicalNode = new PhysicalNode(backendConFactory, 
							poolType, poolName, minCon, maxCon, node.host(), node.port());
					newPhysicalNodes.put(node.id(), physicalNode);
				}
				
				// 2 判断是否有新增，更改，删除节点
				StringBuffer logBuffer = new StringBuffer();		// 节点变更日志
				boolean isNodeAdd = false;
				boolean isNodeDel = false;
				boolean isNodeDiff = false;
				
				// 判断是否有新增和更改节点
				for (Map.Entry<Integer, PhysicalNode> newEntry : newPhysicalNodes.entrySet()) {
					int id = newEntry.getKey();
					PhysicalNode newNode = newEntry.getValue();
					PhysicalNode oldNode = physicalNodes.get(id);
					if (oldNode == null) {
						isNodeAdd = true;
						logBuffer.append( "add newNode:");
						logBuffer.append( newNode.toString() );
						logBuffer.append( "\r\n" );
					} else if (newNode.getPort() != oldNode.getPort() || !newNode.getHost().equals(oldNode.getHost())) {
						isNodeDiff = true;
						logBuffer.append( "dif newNode:");
						logBuffer.append( newNode.toString() );
						logBuffer.append( "\r\n" );
					}
				}
				
				// 判断是否有节点删除
				for (Map.Entry<Integer, PhysicalNode> oldEntry : physicalNodes.entrySet()) {
					int id = oldEntry.getKey();
					PhysicalNode oldNode = oldEntry.getValue();
					PhysicalNode newNode = newPhysicalNodes.get(id);
					if (newNode == null) {
						isNodeDel = true;
						logBuffer.append( "delete oldNode:");
						logBuffer.append( oldNode.toString() );
						logBuffer.append( "\r\n" );
					}
				}
				
				// 集群发生变化， 自动切换
				if ( isNodeAdd || isNodeDel || isNodeDiff ) {
					
					LOGGER.error("kafka node change: heartbeat={}, log={}", heartbeatTime, logBuffer.toString());
					
					// 建立后端连接				
					for (Map.Entry<Integer, PhysicalNode> newEntry : newPhysicalNodes.entrySet()) {
						PhysicalNode node = newEntry.getValue();
						node.initConnections();
					}
					
					// 重新加载kafka topic信息
					try {
						((KafkaPoolCfg) this.poolCfg).reloadExtraCfg();
					} catch (Exception e) {
					}
					
					// 备份old
					Map<Integer, PhysicalNode> oldPhysicalNodes = this.physicalNodes;
					// 切换new
					this.physicalNodes = newPhysicalNodes;
					
					// 清理old
					for (PhysicalNode oldNode : oldPhysicalNodes.values()) {	
						oldNode.clearConnections("this node exception, automatic reload", true);
					}
					oldPhysicalNodes.clear();
				} 
			}
		} finally {
			availableCheckFlag.set( false );
		}

	}

	@Override
	public void heartbeatCheck(long timeout) {
		// 心跳
		for (PhysicalNode physicalNode : physicalNodes.values()) {
			// 心跳检测, 超时抛弃
			// --------------------------------------------------------------------------
			long heartbeatTime = TimeUtil.currentTimeMillis() - timeout;
			long closeTime = TimeUtil.currentTimeMillis() - (timeout * 2);

			LinkedList<BackendConnection> heartBeatCons = getNeedHeartbeatCons(physicalNode.conQueue.getCons(),
					heartbeatTime, closeTime);
			if (!heartBeatCons.isEmpty()) {
				for (BackendConnection conn : heartBeatCons) {
					RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS.id, (short)1, Thread.currentThread().getName(), Utils.getCorrelationId());
					Struct struct = requestHeader.toStruct();
					ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( struct.sizeOf() + 4);
					buffer.putInt(struct.sizeOf());
					struct.writeTo(buffer);
					
					conHeartBeatHanler.doHeartBeat(conn, buffer);
				}
			}
			heartBeatCons.clear();
			conHeartBeatHanler.abandTimeoutConns();

			// 连接池 动态调整逻辑
			// -------------------------------------------------------------------------------
			int idleCons = physicalNode.getIdleCount();
			int activeCons = physicalNode.getActiveCount();
			int minCons = poolCfg.getMinCon();
			int maxCons = poolCfg.getMaxCon();

			if (LOGGER.isDebugEnabled())
				LOGGER.debug("ClusterHeartbeat: host={}, idle={}, active={}, min={}, max={}, lasttime={}",
						new Object[] { physicalNode.getHost() + ":" + physicalNode.getPort(), idleCons, activeCons,
								minCons, maxCons, System.currentTimeMillis() });

			if (idleCons > minCons) {

				if (idleCons < activeCons) {
					return;
				}

				// 闲置太多
				closeByIdleMany(physicalNode, idleCons - minCons);

			} else if (idleCons < minCons) {

				if (idleCons > (minCons * 0.5)) {
					return;
				}

				// 闲置太少
				if ((idleCons + activeCons) < maxCons) {
					int createCount = (int) Math.ceil((minCons - idleCons) / 3F);
					createByIdleLitte(physicalNode, idleCons, createCount);
				}
			}
		}

	}
	
	
    /**
     * 延迟时间统计
     */
    @Override
    public void latencyCheck() {

        PhysicalNode physicalNode;
        for (Integer id : physicalNodes.keySet()) {

            physicalNode = physicalNodes.get(id);

            long requestMilliseconds = System.currentTimeMillis();
            
			KafkaConClient client = null;
			try {

				client = new KafkaConClient(id, physicalNode.getHost(), physicalNode.getPort());
				org.apache.kafka.common.requests.ApiVersionsRequest.Builder build = 
						new org.apache.kafka.common.requests.ApiVersionsRequest.Builder((short) 1);
				ClientRequest clientRequest = client.newClientRequest(build);

				//
				ClientResponse response = client.sendAndRecvice(clientRequest);
				boolean isError = true;
				if (response != null) {
					org.apache.kafka.common.requests.ApiVersionsResponse rs = 
							(org.apache.kafka.common.requests.ApiVersionsResponse) response.responseBody();
					if (rs.error() == Errors.NONE) {
						isError = true;
					}
				}
				
				long cost = System.currentTimeMillis() - requestMilliseconds;
				physicalNode.calcOverload(cost, isError, poolCfg.getMaxLatencyThreshold());

			} catch (IOException e) {
				
				//
				long cost = System.currentTimeMillis() - requestMilliseconds;
				physicalNode.calcOverload(cost, true, poolCfg.getMaxLatencyThreshold());
				
				LOGGER.error("Failed to get latency from kafka server {}", physicalNode.getName());
			} finally {
				if (client != null) {
					client.close();
					client = null;
				}

			}
        }  
    }
    

	@Override
	public PhysicalNode getPhysicalNode(int id) {
		return physicalNodes.get(id);
	}

	public Map<Integer, PhysicalNode> getPhysicalNodes(){
		return physicalNodes;
	}
	
    
}
