package com.feeyo.redis.engine.manage.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.RedisRequest;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.node.ClusterInfo.ClusterState;
import com.feeyo.redis.engine.manage.node.NodeManageRequest.CmdType;
import com.feeyo.redis.net.backend.pool.cluster.ClusterNode;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.backend.pool.cluster.SlotRange;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * 
 * @author yangtao
 *
 */
public class NodeManageService {
	
	protected static Logger LOGGER = LoggerFactory.getLogger(NodeManageService.class);
	
	public static final int SLOTS_COUNT = 16384;
	
	// 集群错误信息
	public static final String SLOTS_MISSING = "#{} slots are not covered by nodes.\r\n";
	public static final String SLOTS_FAIL = "#{} slots are failed.\r\n";
	public static final String SLOTS_PFAIL = "#{} slots are predict to be failed.\r\n";
	
	private static Map<String, ClusterNodeGroup> groupedNodes = new ConcurrentHashMap<String, ClusterNodeGroup>();
	
	public NodeManageService(RedisFrontConnection frontCon) {
		this.discoverAndGroupClusterNodes(frontCon);
	}
	
	/**
	 * 整合request变成 NodeManageRequest
	 * @param request
	 * @return
	 * @throws NodeManageParamException
	 */
	public static NodeManageRequest requestIntegration(RedisRequest request) throws NodeManageParamException {
		byte[][] args = request.getArgs();
		
		/**
			NODE CHECK                                检查集群状态
			NODE ADDMASTER 127.0.0.1:6379                                       添加节点为master节点
			NODE ADDSLAVE  127.0.0.1:6379                                       添加节点为slave节点
			NODE DELETE    127.0.0.1:6379                                       删除节点
			NODE RESHARD   127.0.0.1:6379 1000        为节点分片(最后一个参数是分片得数量)
		 */
		if (args.length < 2) {
			throw new NodeManageParamException("Param is not enough.");
		} else if (args.length == 2) {
			String cmdType = new String(args[1]).toUpperCase();
			if ("CHECK".equals(cmdType)) {
				return new NodeManageRequest(CmdType.NODE_CHECK, null , 0);
			} else if ("ADDMASTER".equals(cmdType) || "ADDSLAVE".equals(cmdType) || "DELETE".equals(cmdType) || "RESHARD".equals(cmdType)) {
				throw new NodeManageParamException("Param error: param is not enouth.");
			} else {
				throw new NodeManageParamException("Param error:" + cmdType + " is not a correct cmd.");
			}
		} else if (args.length == 3) {
			String cmdType = new String(args[1]).toUpperCase();
			if ("ADDMASTER".equals(cmdType) || "ADDSLAVE".equals(cmdType) || "DELETE".equals(cmdType)) {
				String host = new String(args[2]);
				String[] ipAndPort = host.split(":");
				if (ipAndPort.length!=2) {
					throw new NodeManageParamException("Param error: " + host + " is not a correct node.");
				}
				try {
					String ip = ipAndPort[0];
					int port = Integer.parseInt(ipAndPort[1]);
					CmdType type = null;
					switch (cmdType) {
					case "ADDMASTER":
						type = CmdType.NODE_ADDMASTER;
						break;
					case "ADDSLAVE":
						type = CmdType.NODE_ADDSLAVE;
						break;
					case "DELETE":
						type = CmdType.NODE_DELETE;
						break;
					default:
						break;
					}
					return new NodeManageRequest(type, ip , port);
				} catch (Exception e) {
					throw new NodeManageParamException("Param error: " + host + " is not a correct node.");
				}
			} else if ("RESHARD".equals(cmdType)) {
				throw new NodeManageParamException("Param error: no slot range.");
			} else {
				throw new NodeManageParamException("Param error:" + cmdType + " is not a correct cmd.");
			}
		} else if (args.length == 4) {
			String cmdType = new String(args[1]).toUpperCase();
			if ("RESHARD".equals(cmdType)) {
				String host = new String(args[2]);
				String[] ipAndPort = host.split(":");
				if (ipAndPort.length!=2) {
					throw new NodeManageParamException("Param error: " + host + " is not a correct node.");
				}
				String ip;
				int port;
				try {
					ip = ipAndPort[0];
					port = Integer.parseInt(ipAndPort[1]);
				} catch (Exception e) {
					throw new NodeManageParamException("Param error: " + host + " is not a correct node.");
				}
				NodeManageRequest nodeManageRequest = new NodeManageRequest(CmdType.NODE_SLOTE_RESHARD, ip , port);
				
				String args3 = new String(args[3]);
				try {
					int slots = Integer.parseInt(new String(args3));
					nodeManageRequest.setSlots(slots);
				} catch (Exception e) {
					throw new NodeManageParamException("Param error: " + args3 + " is not correct slots.");
				}
				
				return nodeManageRequest;
			} else {
				throw new NodeManageParamException("Param error:" + cmdType + " is not a correct cmd.");
			}
		} else if (args.length > 4) {
			throw new NodeManageParamException("Param error: param is too much.");
		}
		return null;
	}
	
	/**
	 * 执行node命令
	 * @return
	 */
	public  static byte[] excute(NodeManageRequest nodeManageRequest) {
		
		try {
			// 集群健康检查
			clusterHealthyCheck();
			// 节点状态检查
			nodeCheck(nodeManageRequest);
			
			switch (nodeManageRequest.getCmdType()) {
			case NODE_CHECK:
				return checkNode();
			case NODE_ADDMASTER:
			case NODE_ADDSLAVE:
				return addNode(nodeManageRequest, nodeManageRequest.getCmdType() == CmdType.NODE_ADDSLAVE);
			case NODE_DELETE:
				return deleteNode(nodeManageRequest);
			case NODE_SLOTE_RESHARD:
				return reshardSlot(nodeManageRequest);
			default:
				break;
			}
		} catch (ClusterStateException e1) {
			ClusterInfo clusterInfo = e1.getClusterInfo();
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ");
			if (nodeManageRequest.getCmdType() == CmdType.NODE_CHECK) {
				String clusterNodesInfo = getClusterNodesInfo();
				sb.append(clusterNodesInfo);
				sb.append("\n");
				sb.append("\n");
			}
			
			if (clusterInfo == null) {
				sb.append(e1.getMessage());
				sb.append("\r\n");
			} else {
				ClusterState clusterState = clusterInfo.getClusterState();
				switch (clusterState) {
				
				case SLOTS_MISSING:
					sb.append(SLOTS_MISSING.replace("#{}", String.valueOf(SLOTS_COUNT - clusterInfo.getClusterSlotsCount())));
					break;
				case SLOTS_FAIL:
					sb.append(SLOTS_FAIL.replace("#{}", String.valueOf(clusterInfo.getClusterFailSlotsCount())));
					break;
				case SLOTS_PFAIL:
					sb.append(SLOTS_PFAIL.replace("#{}", String.valueOf(clusterInfo.getClusterPFailSlotsCount())));
					break;
				default:
					break;
				}
			}
			return sb.toString().getBytes(); 
		} catch(NodeStateException e2) {
			return e2.getMessage().getBytes(); 
		}
		
		return null;
	}
	
	// NODE CHECK
	private static byte[] checkNode() {
		StringBuffer sb = new StringBuffer();
		String clusterNodesInfo = getClusterNodesInfo();
		sb.append("+");
		sb.append(clusterNodesInfo);
		sb.append("\n");
		sb.append("\n");
		sb.append("This cluster is very health.");
		sb.append("\r\n");
		return sb.toString().getBytes();
	}
	
	/**
	 * 获取 cluster nodes信息
	 * @return
	 */
	private static String getClusterNodesInfo() {
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			ClusterNode masterNode = entry.getValue().getMaster();
			String result = blockWriteToBackendAndBulkReply(masterNode.getHost(), masterNode.getPort(), RedisCommand.CLUSTER, "nodes");
			return result;
		}
		return null;
	}
	

	// 添加节点
	private static byte[] addNode(NodeManageRequest nodeManageRequest, boolean isSlave) {
		String ip = nodeManageRequest.getIp();
		int port = nodeManageRequest.getPort();
		
		String masterId = findMasterIdForNewSlave();
		ClusterNodeGroup clusterNodeGroup = groupedNodes.get(masterId);
		ClusterNode masterNode = clusterNodeGroup.getMaster();
		
		// cluster meet 127.0.0.1:6379
		String result = blockWriteToBackendAndBulkReply(masterNode.getHost(), masterNode.getPort(), RedisCommand.CLUSTER, "MEET", ip, String.valueOf(port));
		if ( !isOk(result) ) 
			return (result == null ? "" : result).getBytes();
		
		// 如果是添加slave节点
		if (isSlave) {
			// 参考redis-trib做法
			waitClusterJoin();
			result = blockWriteToBackendAndBulkReply(ip, port, RedisCommand.CLUSTER, "REPLICATE", masterId);
			
			if ( !isOk(result) ) 
				return (result == null ? "" : result).getBytes();
		}
	
		return "+Node add ok.\r\n".getBytes();
	}
	
	// 删除节点
	private static byte[] deleteNode(NodeManageRequest nodeManageRequest) {
			
		String nodeId = nodeManageRequest.getId();
		ClusterNodeGroup clusterNodeGroup = groupedNodes.get(nodeId);

		// 如果此节点是master节点，为其slave节点寻找新的master
		if (clusterNodeGroup != null) {
			List<ClusterNode> slaveNodes = clusterNodeGroup.getSlaves();
			for (ClusterNode slave : slaveNodes) {
				String masterId = findAnotherMasterIdForSlave(nodeId);
				String result = blockWriteToBackendAndBulkReply(slave.getHost(), slave.getPort(), RedisCommand.CLUSTER, "REPLICATE",
						masterId);
				
				if ( !isOk(result) ) 
					return (result == null ? "" : result).getBytes();
			}
		}

		// 循环集群忘记节点, 自己不能忘记自己
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			clusterNodeGroup = entry.getValue();
			ClusterNode masterNode = clusterNodeGroup.getMaster();

			if (!nodeId.equals(masterNode.getId())) {
				String result = blockWriteToBackendAndBulkReply(masterNode.getHost(), masterNode.getPort(), RedisCommand.CLUSTER,
						"FORGET", nodeId);
				
				if ( !isOk(result) ) 
					return (result == null ? "" : result).getBytes();
			}

			List<ClusterNode> slaveNodes = clusterNodeGroup.getSlaves();
			for (ClusterNode slave : slaveNodes) {
				if (!nodeId.equals(slave.getId())) {
					String result = blockWriteToBackendAndBulkReply(slave.getHost(), slave.getPort(), RedisCommand.CLUSTER,
							"FORGET", nodeId);
					
					if ( !isOk(result) ) 
						return (result == null ? "" : result).getBytes();
				}
			}
		}
			
		return "+Node delete ok.\r\n".getBytes();
	}
	
	/**
	 * 分片
	 * @param nodeManageRequest
	 * @return
	 */
	private static byte[] reshardSlot(NodeManageRequest nodeManageRequest) {
		// 需要转移的slots数量
		int removeSlots = nodeManageRequest.getSlots();
		String targetNodeId = nodeManageRequest.getId();
		
		// 计算每个节点需要迁移的slots
		Map<String, Integer> slotsMap = calculateRemoveSlotsForMasterNode(targetNodeId, removeSlots);
		
		for (Entry<String, Integer> entry : slotsMap.entrySet()) {
			// 源节点id
			String sourceNodeId = entry.getKey();
			ClusterNode sourceNode = groupedNodes.get(sourceNodeId).getMaster();
			int slots = entry.getValue();
			// 迁移源节点的slots到目标节点
			try {
				removeSlots(slots, sourceNode, nodeManageRequest);
			} catch (SlotRemoveException e) {
				return e.getMessage().getBytes();
			}
		}
		
		return "+OK.\r\n".getBytes();
	}

	/**
	 * 计算每一个master节点需要迁移的slots分组
	 * @param targetNodeId迁移的目标nodeid
	 */
	private static Map<String, Integer> calculateRemoveSlotsForMasterNode(String targetNodeId, int removeSlots) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		
		int totalSlots = 0;
		// 先把每个节点包含的分片数量提取出来
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			if (targetNodeId.equals(entry.getKey())) {
				continue;
			}
			int slots = 0;
			ClusterNode masterNode = entry.getValue().getMaster();
			List<SlotRange> slotRanges = masterNode.getSlotRanges();
			for (SlotRange slotRange : slotRanges) {
				int start = slotRange.getStart();
				int end = slotRange.getEnd();
				slots += end - start + 1;
			}
			totalSlots += slots;
			result.put(masterNode.getId(), slots);
		}
		
		boolean isNeedCeil = true;
		// 计算每个节点需要迁移的slots
		for (Entry<String, Integer> entry : result.entrySet()) {
			// 迁移slot数量 * (该源节点负责的slot数量 / 源节点列表负责的slot总数)
			double slots = removeSlots * ( (double)entry.getValue() / (double)totalSlots );
			// 可能会出现小数，所以第一个ceil，其他floor
			if (isNeedCeil) {
				entry.setValue( (int) Math.ceil(slots) > entry.getValue() ? entry.getValue() : (int) Math.ceil(slots) );
				isNeedCeil = false;
			} else {
				entry.setValue( (int) Math.floor(slots) > entry.getValue() ? entry.getValue() : (int) Math.floor(slots) );
			}
		}
		
		return result;
	}
	
	/**
	 * 
	 * @param slots  需要迁移的slots数量
	 * @param sourceNode 源节点
	 * @param targetNode 目的节点
	 */
	private static void removeSlots(int slots, ClusterNode sourceNode, NodeManageRequest nodeManageRequest)
			throws SlotRemoveException {
		String targetNodeId = nodeManageRequest.getId();
		String targetNodeIP = nodeManageRequest.getIp();
		int targetNodePort = nodeManageRequest.getPort();
		
		String sourceNodeId = sourceNode.getId();
		String sourceNodeIP = sourceNode.getHost();
		int sourceNodePort = sourceNode.getPort();
		
		List<SlotRange> sourceSlotRanges = sourceNode.getSlotRanges();
		removeSlots : for (SlotRange slotRange : sourceSlotRanges) {
			int slotStart = slotRange.getStart();
			int slotEnd = slotRange.getEnd();
			for (int i = slotStart; i <= slotEnd; i++) {

				// CLUSTER SETSLOT <slot> MIGRATING <node_id> 将本节点的槽 slot 迁移到
				// node_id 指定的节点中。
				String result = blockWriteToBackendAndBulkReply(sourceNodeIP, sourceNodePort,
						RedisCommand.CLUSTER, "SETSLOT", String.valueOf(i), "MIGRATING", targetNodeId);
				
				if ( !isOk(result) ) 
					throw new SlotRemoveException( (result == null ? "" : result) );
				// CLUSTER SETSLOT <slot> IMPORTING <node_id> 从 node_id
				// 指定的节点中导入槽 slot 到本节点。
				result = blockWriteToBackendAndBulkReply(targetNodeIP, targetNodePort, RedisCommand.CLUSTER, "SETSLOT",
						String.valueOf(i), "IMPORTING", sourceNodeId);
				
				if ( !isOk(result) ) 
					throw new SlotRemoveException( (result == null ? "" : result) );

				// 如果这个slot中有数据
				for (;;) {
					// CLUSTER GETKEYSINSLOT <slot> <count> 返回 count 个 slot
					// 槽中的键。
					List<String> keys = blockWriteToBackendAndMultiBulkReply(sourceNodeIP, sourceNodePort,
							RedisCommand.CLUSTER, "GETKEYSINSLOT", String.valueOf(i), "20");
					if (keys == null || keys.size() == 0) {
						break;
					}
					for (String key : keys) {
						// MIGRATE host port key destination-db timeout [COPY] [REPLACE]
						// 将 key 原子性地从当前实例传送到目标实例的指定数据库上，一旦传送成功， key 保证会出现在目标实例上，而当前实例上的 key 会被删除。
						result = blockWriteToBackendAndBulkReply(sourceNodeIP, sourceNodePort,
								RedisCommand.MIGRATE, targetNodeIP, String.valueOf(targetNodePort), key, "0", "1000");
						
						if ( !isOk(result) ) 
							throw new SlotRemoveException( (result == null ? "" : result) );
					}
				}

				// CLUSTER SETSLOT <slot> NODE <node_id> 将槽 slot 指派给 node_id 指定的节点。
				for (Entry<String, ClusterNodeGroup> entry1 : groupedNodes.entrySet()) {
					ClusterNode masterNode = entry1.getValue().getMaster();
					result = blockWriteToBackendAndBulkReply(masterNode.getHost(), masterNode.getPort(),
							RedisCommand.CLUSTER, "SETSLOT", String.valueOf(i), "NODE", targetNodeId);
					
					if ( !isOk(result) ) 
						throw new SlotRemoveException( (result == null ? "" : result) );
				}

				slots--;
				if (slots < 0) {
					break removeSlots;
				}
			}

		}
	}
	
	
	/** cluster info
	 	cluster_state:ok              
		cluster_slots_assigned:16384     #已分配的槽
		cluster_slots_ok:16384           #槽的状态是ok的数目
		cluster_slots_pfail:0            #可能失效的槽的数目
		cluster_slots_fail:0             #已经失效的槽的数目
		cluster_known_nodes:6            #集群中节点个数
		cluster_size:3
		cluster_current_epoch:8
		cluster_my_epoch:1
		cluster_stats_messages_sent:7284
		cluster_stats_messages_received:6996
	 */
	private static void clusterHealthyCheck() throws ClusterStateException {
		
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			ClusterNodeGroup clusterNodeGroup = entry.getValue();
			ClusterNode masterNode = clusterNodeGroup.getMaster();
			
			ClusterState clusterState = null;
			int clusterSlotsCount = 0;
			int clusterOkSlotsCount = 0;
			int clusterFailSlotsCount = 0;
			int clusterPFailSlotsCount = 0;
			String state = null;
			
			try {
				String clusterInfoReply = blockWriteToBackendAndBulkReply(masterNode.getHost(), masterNode.getPort(), RedisCommand.CLUSTER, "INFO");
				String[] clusterInfoArray = clusterInfoReply.split("\r\n");
				for (String clusterInfo : clusterInfoArray) {
					String[] info = clusterInfo.split(":");
					if ("cluster_state".equals(info[0])) {
						state = info[1];
					}
					if ("cluster_slots_assigned".equals(info[0])) {
						clusterSlotsCount = Integer.parseInt(info[1]);
					}
					if ("cluster_slots_ok".equals(info[0])) {
						clusterOkSlotsCount = Integer.parseInt(info[1]);
					}
					if ("cluster_slots_fail".equals(info[0])) {
						clusterFailSlotsCount = Integer.parseInt(info[1]);
					}
					if ("cluster_slots_pfail".equals(info[0])) {
						clusterPFailSlotsCount = Integer.parseInt(info[1]);
					}
				}
			} catch(Exception e) {
				throw new ClusterStateException("This cluster is Unhealthy.", null);
			}
				
			if (isOk(state)) {
				clusterState = ClusterState.OK;
			} else {
				if (clusterSlotsCount != SLOTS_COUNT) {
					clusterState = ClusterState.SLOTS_MISSING;
				} 
				if (clusterPFailSlotsCount != 0) {
					clusterState = ClusterState.SLOTS_PFAIL;
				}
				if (clusterFailSlotsCount != 0) {
					clusterState = ClusterState.SLOTS_FAIL;
				}
				ClusterInfo result = new ClusterInfo(clusterState, clusterSlotsCount, clusterOkSlotsCount,
						clusterFailSlotsCount, clusterPFailSlotsCount);
				throw new ClusterStateException("This cluster is Unhealthy.", result);
			}
			break;
		}
	}
	
	private static void nodeCheck(NodeManageRequest nodeManageRequest) throws NodeStateException {
		
		String ip = nodeManageRequest.getIp();
		int port = nodeManageRequest.getPort();
		
		switch (nodeManageRequest.getCmdType()) {
		case NODE_CHECK:
			break;
		case NODE_ADDMASTER:
		case NODE_ADDSLAVE:
			// 节点开启 && 是集群配置 && 不在一个集群中
			String reply = blockWriteToBackendAndBulkReply(ip, port, RedisCommand.PING);
			if (!"PONG".equals(reply)) {
				throw new NodeStateException("-ERR Can not connect to node.\r\n");
			}
			reply = blockWriteToBackendAndBulkReply(ip, port, RedisCommand.CLUSTER, "INFO");
			if (reply == null) {
				throw new NodeStateException("-ERR Can not connect to node.\r\n");
			}
			if (reply.contains("ERR")) {
				throw new NodeStateException("-ERR Is not configured as a cluster node.\r\n");
			}
			if (!reply.contains("cluster_known_nodes:1")) {
				throw new NodeStateException("-ERR The node already knows other nodes.\r\n");
			}
			break;
		case NODE_DELETE:
			
			// 节点在集群内 && （是slave节点 || （master节点  && slot为空））
			for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
				ClusterNodeGroup clusterNodeGroup = entry.getValue();
				ClusterNode masterNode = clusterNodeGroup.getMaster();
				if (masterNode.getHost().equals(ip) && masterNode.getPort() == port) {
					if (masterNode.getSlotRanges().size() == 0) {
						// 把次节点的id放入nodeManageRequest
						nodeManageRequest.setId(masterNode.getId());
						return;
					} else {
						throw new NodeStateException("-ERR Node is not empty.\r\n");
					}
				}
				
				List<ClusterNode> slaveNodes = clusterNodeGroup.getSlaves();
				for (ClusterNode slaveNode : slaveNodes) {
					if (slaveNode.getHost().equals(ip) && slaveNode.getPort() == port) {
						// 把次节点的id放入nodeManageRequest
						nodeManageRequest.setId(slaveNode.getId());
						return;
					}
				}
			}
			throw new NodeStateException("-ERR Node is not in the cluster.\r\n");
		case NODE_SLOTE_RESHARD:
		
			// 在集群中 && 是master节点
			for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
				ClusterNodeGroup clusterNodeGroup = entry.getValue();
				ClusterNode masterNode = clusterNodeGroup.getMaster();
				if (masterNode.getHost().equals(ip) && masterNode.getPort() == port) {
					nodeManageRequest.setId(masterNode.getId());
					return;
				}
				
				List<ClusterNode> slaveNodes = clusterNodeGroup.getSlaves();
				for (ClusterNode slaveNode : slaveNodes) {
					if (slaveNode.getHost().equals(ip) && slaveNode.getPort() == port) {
						throw new NodeStateException("-ERR This node is not master node.\r\n");
					}
				}
			}
			throw new NodeStateException("-ERR Node is not in the cluster.\r\n");
		default:
			break;
		}
		
	}
	
	/**
		127.0.0.1:6379> cluster nodes
		44d8cc00c247996e44f2631da5bbfa8383ea7871 127.0.0.1:7000 slave 18b98ab3ec5afb63c25e26ef172b1415dc72843e 0 1482834595597 10 connected
		4b3d7557009a6e4e831aff9b6c6c9ec2e06f53c5 127.0.0.1:7001 myself,slave 2fa5e82a0d41eba6251450e248e1efa27a5de3a4 0 0 3 connected
		1a2f5c5898fda35087cc077146e7e903797f2c47 127.0.0.1:7002 slave 9dcca087ddbe9f621717fb2e9f5165cb2d61794e 0 1482834596600 8 connected
		ecf2e4e9f53e24c359c1def71c032219c8887a2e 127.0.0.1:7003 master - 0 1482834597100 11 connected 0-1364 5461-6826 10923-12287
		18b98ab3ec5afb63c25e26ef172b1415dc72843e 127.0.0.1:7004 master - 0 1482834597601 10 connected 6827-10922
		9dcca087ddbe9f621717fb2e9f5165cb2d61794e 127.0.0.1:7005 master - 0 1482834596098 8 connected 12288-16383
		2fa5e82a0d41eba6251450e248e1efa27a5de3a4 127.0.0.1:7006 master - 0 1482834596600 6 connected 1365-5460
	 */
	private void discoverAndGroupClusterNodes(RedisFrontConnection frontCon) {
		RedisClusterPool pool = (RedisClusterPool) RedisEngineCtx.INSTANCE().getPoolMap()
				.get(frontCon.getUserCfg().getPoolId());
		
		// cluster nodes
		List<String> theHostList = pool.getNodes();

		for (String availableHost : theHostList) {

			String[] hostAndPort = availableHost.split(":");

			JedisConnection conn = null;
			try {
				conn = new JedisConnection(hostAndPort[0], Integer.parseInt(hostAndPort[1]), 5000, 0);
				conn.sendCommand(RedisCommand.CLUSTER, "nodes");
				String nodeInfoReply = conn.getBulkReply();
				String[] nodeInfoArray = nodeInfoReply.split("\n");
				for (String nodeInfo : nodeInfoArray) {

					String[] fields = nodeInfo.split(" ");

					ClusterNode node = new ClusterNode();
					node.setId(fields[0]);

					// 主机
					String host = fields[1].split(":")[0];
					if (host.equals("127.0.0.1") || host.startsWith("localhost") || host.equals("0.0.0.0")
							|| host.startsWith("169.254") || host.startsWith("::1")
							|| host.startsWith("0:0:0:0:0:0:0:1")) {
						node.setHost( RedisClusterPool.LOCALHOST_STR );
					} else {
						node.setHost(host);
					}

					// 端口
					node.setPort(Integer.parseInt(fields[1].split(":")[1]));

					// 标示、可用状态
					node.setFlagInfo(fields[2]);
					if (fields[2].indexOf("master") > -1) {
						node.setType("master");
					} else {
						node.setType("slave");
					}

					if (fields[2].indexOf("fail") > -1) {
						node.setFail(true);
					} else {
						node.setFail(false);
					}

					// slave 获取 masterId
					if (!"-".equals(fields[3])) {
						node.setMasterId(fields[3]);
					} else {
						node.setMasterId(node.getId());
					}

					// 连接状态
					node.setConnectInfo(fields[7]);
					if (fields[7].indexOf("connected") > -1) {
						node.setConnected(true);
					} else {
						node.setConnected(false);
					}

					// 多个slot 段位处理
					if (fields.length > 8) {
						int slotIndex = 8;
						while (slotIndex < fields.length) {
							List<SlotRange> ranges = new ArrayList<SlotRange>();
							String slotInfo = fields[slotIndex];
							String[] slotRangeArray = slotInfo.split(",");
							for (String slotRange : slotRangeArray) {
								String[] slot = slotRange.split("-");
								ranges.add(new SlotRange(new Integer(slot[0]), new Integer(slot[1])));
							}
							node.getSlotRanges().addAll(ranges);
							slotIndex++;
						}
					}
					this.groupNode(node);
				}

				// 只需一个node 查询成功, 即跳出
				break;

			} catch (JedisConnectionException e) {
				LOGGER.error("discover cluster err:", e);
			} finally {
				if (conn != null) {
					conn.disconnect();
				}
			}
		}
	}
	
	/**
	 * redis 节点分组
	 * @param node
	 */
	private void groupNode(ClusterNode node) {
		String masterId = node.getMasterId();
		ClusterNodeGroup clusterNodeGroup = groupedNodes.get(masterId);
		if (clusterNodeGroup == null) {
			clusterNodeGroup = new ClusterNodeGroup();  
			groupedNodes.put(masterId, clusterNodeGroup);
		}
		if (masterId.equals(node.getId())) {
			clusterNodeGroup.setMaster(node);
		} else {
			clusterNodeGroup.addSlave(node);
		}
	}
	
	/**
	 * 为新加入的slave节点寻找master节点
	 * 寻找master节点中slave节点数最少的master节点。如果有slave节点一样多的情况，选择第一个遍历的master节点。 
	 * @return
	 */
	private static String findMasterIdForNewSlave() {
		int slaveCount = 0;
		String masterId = null;
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			ClusterNodeGroup clusterNodeGroup = entry.getValue();
			if (slaveCount == 0 || clusterNodeGroup.getSlaves().size() < slaveCount) {
				slaveCount = clusterNodeGroup.getSlaves().size();
				masterId = clusterNodeGroup.getMaster().getId();
			} 
		}
		return masterId;
	}
	
	/**
	 * 为slave节点寻找新的master节点
	 * 寻找排除老的master节点外的master节点中，slave节点数最少的master节点。如果有slave节点一样多的情况，选择第一个遍历的master节点。 
	 * @return
	 */
	private static String findAnotherMasterIdForSlave(String oldMasterId) {
		int slaveCount = 0;
		String masterId = null;
		for (Entry<String, ClusterNodeGroup> entry : groupedNodes.entrySet()) {
			if (oldMasterId.equals(entry.getKey())) {
				continue;
			}
			ClusterNodeGroup clusterNodeGroup = entry.getValue();
			if (slaveCount == 0 || clusterNodeGroup.getSlaves().size() < slaveCount) {
				slaveCount = clusterNodeGroup.getSlaves().size();
				masterId = clusterNodeGroup.getMaster().getId();
			} 
		}
		return masterId;
	}
	
	// 阻塞的写入后端
	private static String blockWriteToBackendAndBulkReply(String ip, int port, final RedisCommand cmd, final String... args) {
		JedisConnection conn = null;
		try {
			conn = new JedisConnection(ip, port, 5000, 0);
			conn.sendCommand(cmd, args);
			String nodeInfoReply = conn.getBulkReply();
			return nodeInfoReply;
		} catch (JedisConnectionException e) {
			LOGGER.error("", e);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return null;
	}
	
	// 阻塞的写入后端
	private static List<String> blockWriteToBackendAndMultiBulkReply(String ip, int port, final RedisCommand cmd, final String... args) {
		JedisConnection conn = null;
		try {
			conn = new JedisConnection(ip, port, 5000, 0);
			conn.sendCommand(cmd, args);
			List<String> nodeInfoReply = conn.getMultiBulkReply();
			return nodeInfoReply;
		} catch (JedisConnectionException e) {
			LOGGER.error("", e);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return null;
	}
	
	private static void waitClusterJoin() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}
	
	private static boolean isOk(String result) {
		result = result == null ? "" : result;
		if ("OK".equals(result.toUpperCase())) 
			return true;
		return false;
	}
	
	class ClusterNodeGroup {
		private ClusterNode master;
		private List<ClusterNode> slaves;
		
		public ClusterNodeGroup() {
			slaves = new ArrayList<ClusterNode>();
		}
		
		public void setMaster(ClusterNode master) {
			this.master = master;
		}
		
		public ClusterNode getMaster() {
			return master;
		}
		
		public List<ClusterNode> getSlaves() {
			return slaves;
		}
		
		public void addSlave(ClusterNode slave) {
			this.slaves.add(slave);
		}
	}
	
}
