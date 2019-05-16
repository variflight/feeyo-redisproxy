package com.feeyo.redis.net.backend.pool.cluster;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.ConHeartBeatHandler;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;
import com.google.common.collect.Sets;

import java.net.InetAddress;
import java.util.*;

/**
 * 新的实现，去除了对 slave 关系的维护
 * 
 * @author zhuam 
 */

public class RedisClusterPool extends AbstractPool {
	
	protected ConHeartBeatHandler conHeartBeatHanler = new ConHeartBeatHandler();
	protected RedisBackendConnectionFactory backendConFactory = new RedisBackendConnectionFactory();
	
	public static final String LOCALHOST_STR = getLocalHostQuietly();
	
	public volatile int heartbeatStatus = 1;	
	public volatile long heartbeatTime = -1;
	
	/**
	 * 主节点
	 */
	private Map<String, ClusterNode> masters = new HashMap<String, ClusterNode>(6);
	
	/**
	 * cluster把所有的物理节点映射到[0-16383]slot 的数组
	 * masterIds[ slot ] = masterId
	 */
	private String[] masterIds = new String[ 16384 ];				
	
	
	/**
	 * available node list
	 */
	private Set<String> availableHostList = Sets.newConcurrentHashSet(); //new LinkedHashSet<String>();
	private Set<String> backupHostList = Sets.newConcurrentHashSet();	// 极端情况下的一种防护措施
	
	public RedisClusterPool(PoolCfg poolCfg) {
		
		super( poolCfg );
		
		// 初始化有效的节点列表
		this.availableHostList.addAll(  poolCfg.getNodes() );		
		this.backupHostList.addAll( poolCfg.getNodes() );
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
	private List<ClusterNode> discoverClusterNodes() {
		
		// cluster nodes
		List<ClusterNode> nodes = new ArrayList<ClusterNode>();
		
		Set<String> theHostList = availableHostList;
		if ( theHostList.isEmpty() ) {
			theHostList = backupHostList;
		}
		
		for(String availableHost: theHostList) {
			
			String[] hostAndPort = availableHost.split(":");
			
			JedisConnection conn = null;
			try {				
				conn = new JedisConnection( hostAndPort[0],  Integer.parseInt(  hostAndPort[1] ), 5000, 0);
				conn.sendCommand( RedisCommand.CLUSTER, "nodes");
				String nodeInfoReply = conn.getBulkReply();
				String[] nodeInfoArray = nodeInfoReply.split("\n");
				for (String nodeInfo : nodeInfoArray) {
					
					String[] fields = nodeInfo.split(" ");	
					
					ClusterNode node = new ClusterNode();
					node.setId( fields[0] );
					
					// 主机
					String host = fields[1].split(":")[0];
					
					// f49a659cc20a4ce4db40cb16fa2f058f9a506872 :0 slave,fail,noaddr a896c8380599f4344ac89c24dd43a319b9239e6f 1504410337768 1504410336693 8 disconnected
					if (host == null || "".equals(host.trim())) {
						// cluster nodes会有上面的那种数据，直接跳过循环
						continue;
					}
					
					if (host.equals("127.0.0.1") 
							|| host.startsWith("localhost") 
							|| host.equals("0.0.0.0")
							|| host.startsWith("169.254") 
							|| host.startsWith("::1") 
							|| host.startsWith("0:0:0:0:0:0:0:1")) {
						node.setHost( LOCALHOST_STR );
					} else {
						node.setHost( host );
					}
					
					// 端口
					String port = fields[1].split(":")[1];
					if (port.contains("@")) {
						port =  port.split("@")[0];
					}
					node.setPort(Integer.parseInt(port));
					
					// 标示、可用状态
					node.setFlagInfo( fields[2] );					
					if ( fields[2].indexOf("master") > -1 ) {
						node.setType("master");						
					} else {
						node.setType("slave");						
					}

					if ( fields[2].indexOf("fail") > -1 ) {
						node.setFail( true );						
					} else {
						node.setFail( false );						
					}
					
					// slave 获取 masterId
					if ( !"-".equals(fields[3]) ) {
						node.setMasterId(fields[3]);
					} else {
						node.setMasterId( node.getId() );
					}
					
					// 连接状态
					node.setConnectInfo( fields[7] );					
					if ( fields[7].indexOf("disconnected") > -1 ) {
						node.setConnected( false );
					} else if ( fields[7].indexOf("connected") > -1 )  {
						node.setConnected( true );
					} else {
						node.setConnected( false );
					}
					
					// 多个slot 段位处理
					if (fields.length > 8) {
						int slotIndex = 8;
						while ( slotIndex  < fields.length ) {					
							List<SlotRange> ranges = new ArrayList<SlotRange>();							
							String slotInfo = fields[ slotIndex ];
							
							// 迁移slot时有这种数据 
							// 2626347285976ee42d5c8f923cd87f0f8ddc04e7 192.168.219.136:7000 myself,master - 0 0 1 connected 10-5460 [10->-9ba2af94af911cbbe8ca2e19c689185dbd87936c]
							if (slotInfo.contains("->-")) {
								slotIndex++;
								continue;
							}
							String[] slotRangeArray = slotInfo.split(",");
							for (String slotRange : slotRangeArray) {
								if ( slotRange.contains("-") ) {
									String[] slot = slotRange.split("-");
									ranges.add( new SlotRange( new Integer(slot[0]), new Integer(slot[1]) ) );
								} else {
									ranges.add( new SlotRange( new Integer(slotRange), new Integer(slotRange) ) );
								}
							}	
							node.getSlotRanges().addAll( ranges );
							slotIndex++;
						}
					}					
		            nodes.add( node );
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
		
		// 更新有效主机信息, 便于下次探测
		if ( !nodes.isEmpty() ) {			
			availableHostList.clear();		
			
			for(ClusterNode node: nodes) {
				if ( !node.isConnected() || node.isFail() ) {					
					LOGGER.error("cluster node err: {}", node.toString());	
				} else {
					availableHostList.add( node.getHost() + ":" + node.getPort() );
				}
			}
		}
		return nodes;
	}
	
	@Override
	public boolean startup() {	
		
		// 基于配置 自动发现 NODES 		
		List<ClusterNode> clusterNodes = this.discoverClusterNodes();
		for(ClusterNode clusterNode: clusterNodes) {			
			
			// 构造  masters
			if ( clusterNode.getType().equalsIgnoreCase("master") ) {
			
				String masterId = clusterNode.getId();
				masters.put(masterId , clusterNode);
				
				// 构造 基于 slots 做index 的 masterId 数组				
				for(SlotRange slotRange: clusterNode.getSlotRanges() ) {
					for(int i = slotRange.getStart(); i <= slotRange.getEnd(); i++) {
						masterIds[i] = masterId;
					}
				}
				
				// 初始化 master 后端连接				
				int type = poolCfg.getType();
				String name = poolCfg.getName();
				int minCon = poolCfg.getMinCon();
				int maxCon = poolCfg.getMaxCon();
				
				String host = clusterNode.getHost();
				int port = clusterNode.getPort();
				
				PhysicalNode physicalNode = new PhysicalNode(backendConFactory, type, name, minCon, maxCon, host, port );
				physicalNode.initConnections();
				clusterNode.setPhysicalNode(physicalNode);				
			}			
		}
		return true;
	}
	
	@Override
	public boolean close(boolean isForce) {
		for (ClusterNode clusterNode : masters.values()) {
			clusterNode.getPhysicalNode().clearConnections("manual reload", isForce);
		}
		return true;
	}

	private Random random = new Random();
	
	@Override
	public PhysicalNode getPhysicalNode() {		
		int slot = random.nextInt( 16383 );
		return getPhysicalNodeBySlot( slot );	// default slot 0
	}

	@Override
	public PhysicalNode getPhysicalNode(String cmd, String key) {
		int slot = 0;
		if( key != null ) {
			 slot = ClusterCRC16Util.getSlot( key );
		}
		return getPhysicalNodeBySlot( slot );
	}
	
	@Override
	public PhysicalNode getPhysicalNode(String cmd, byte[] key) {
		int slot = 0;
		if( key != null ) {
			 slot = ClusterCRC16Util.getSlot( key );
		}
		PhysicalNode node = getPhysicalNodeBySlot( slot );
		return node;
	}
	
	public PhysicalNode getPhysicalNodeBySlot(int slot) {
		
		// 根据连接池状态返回
		if ( heartbeatStatus == -1 ) {
			return null;
		}
		
		String masterId = this.masterIds[ slot ];
		ClusterNode master = this.masters.get( masterId );
		if ( master == null ) {
			return null;
		}
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("Get physical node {}:{}, masterId={}, slot={} " , new Object[] { master.getHost(), master.getPort(), masterId, slot } );
		}
		
		if ( master.isConnected() && !master.isFail() ) {
			return master.getPhysicalNode();
		}	
		
		LOGGER.error("Get physical node err: slot={}, masterId={}, master={}",
				new Object[]{ slot, masterId, master } );
		
		return null;
	}

	@Override
	public boolean testConnection() {		
		boolean result = true;		
		List<ClusterNode> clusterNodes = discoverClusterNodes();
		for(ClusterNode clusterNode: clusterNodes) {	
			if ( clusterNode.getType().equalsIgnoreCase("master") ) {
				String connectInfo = clusterNode.getConnectInfo();
				if ( connectInfo.indexOf("connected") == -1 ) {
					LOGGER.error("test connection err: {}",  clusterNode);
					result = false;
					break;
				}
			}
		}
		return result;
	}

	@Override
	public void availableCheck() {
		
		heartbeatTime = System.currentTimeMillis();
		
		// CAS， 避免网络不好的情况下，频繁并发的检测
		if ( !availableCheckFlag.compareAndSet(false,  true) ) {
			return;
		}
		
		try {
			//检测集群是否正常
			List<ClusterNode> clusterNodes = discoverClusterNodes();
			if ( clusterNodes.size() < 3 ) {
				heartbeatStatus = -1;
				LOGGER.error("redis pool err: heartbeatStatus={}", heartbeatStatus);
				
			} else {
				heartbeatStatus = 1;	
				
				/**
				 *  新的节点信息
				 */
				Map<String, ClusterNode> newMasters = new HashMap<String, ClusterNode>(6);
				String[] newMasterIds = new String[ 16384 ];	
				
				/**
				 * 1、提取所有的 master
				 */
				for(ClusterNode clusterNode: clusterNodes) {	
					
					// 构造  newMasters
					if ( clusterNode.getType().equalsIgnoreCase("master") ) {
						String masterId = clusterNode.getId();
						newMasters.put(masterId, clusterNode);
						
						// 构造 newMasterIds
						for(SlotRange slotRange: clusterNode.getSlotRanges() ) {
							for(int i = slotRange.getStart(); i <= slotRange.getEnd(); i++) {
								newMasterIds[i] = masterId;
							}
						}
					}			
				}	
				
				/**
				 * 2、判断是否有新增 master 、移除 master
				 * 3、判断 master slot range 是否发生变化
				 * 4、判断 master connection 状态
				 */			
				// 2.1 检查新增节点
				StringBuffer logBuffer = new StringBuffer();		// 节点变更日志
				boolean isNodeAdd = false;
				boolean isNodeDel = false;
				boolean isNodeSoltDiff = false;
						
				for (Map.Entry<String, ClusterNode> newEntry : newMasters.entrySet()) {
					String masterId = newEntry.getKey();
					ClusterNode newMaster = newEntry.getValue();
					
					ClusterNode oldMaster = masters.get( masterId );
					if ( oldMaster == null ) {
						isNodeAdd = true;
						
						logBuffer.append( "add newNode:");
						logBuffer.append( newMaster.toString() );
						logBuffer.append( "\r\n" );
						
					} else {
						// 状态信息更新
						oldMaster.setConnectInfo( newMaster.getConnectInfo()  );
						oldMaster.setConnected( newMaster.isConnected()  );
						oldMaster.setFlagInfo( newMaster.getFlagInfo() );
						oldMaster.setFail( newMaster.isFail() );
	
						// 校验 slot 是否发生变化
						List<SlotRange> oldSlotRanges = oldMaster.getSlotRanges();				
						List<SlotRange> newSlotRanges = newMaster.getSlotRanges();
						
						if ( oldSlotRanges.size() == newSlotRanges.size() ) {				
							
							for(SlotRange s1 : oldSlotRanges) {			
								boolean isEq = false;
								for(SlotRange s2 : newSlotRanges) {
									if ( s1.getStart() == s2.getStart() && s1.getEnd() == s2.getEnd() ) {
										isEq = true;
									}
								}	
								
								if ( !isEq ) {
									isNodeSoltDiff = true;		
									
									logBuffer.append("diff slotNode:");
									logBuffer.append(" oldMaster=").append( oldMaster.toString() ).append("\r\n");
									logBuffer.append(" newMaster=").append( newMaster.toString() ).append("\r\n");
								} 
								isEq = false;
							}						
						} else {
							isNodeSoltDiff = true;
						}
					}
				}
				
				for (Map.Entry<String, ClusterNode> oldEntry : masters.entrySet()) {
					String masterId = oldEntry.getKey();	
					ClusterNode oldMaster = oldEntry.getValue();
					ClusterNode newMaster = newMasters.get( masterId );
					if ( newMaster == null ) {
						isNodeDel = true;
						
						logBuffer.append( "delete oldNode:");
						logBuffer.append( oldMaster.toString() );
						logBuffer.append( "\r\n" );
					}
				}
				
				// 集群发生变化， 自动切换
				if ( isNodeAdd || isNodeDel || isNodeSoltDiff ) {
					
					LOGGER.error("ClusterChange: heartbeat={}, log={}", heartbeatTime, logBuffer.toString());
					
					// 建立 master 后端连接				
					for (ClusterNode clusterNode : newMasters.values()) {					
						int type = poolCfg.getType();
						String name = poolCfg.getName();
						int minCon = poolCfg.getMinCon();
						int maxCon = poolCfg.getMaxCon();
						
						String host = clusterNode.getHost();
						int port = clusterNode.getPort();
							
						PhysicalNode physicalNode = new PhysicalNode(backendConFactory, type, name, minCon, maxCon, host, port );
						physicalNode.initConnections();
						clusterNode.setPhysicalNode( physicalNode );
					}
					
					// 备份old
					Map<String, ClusterNode> oldMasters = this.masters;
					
					// 切换new
					this.masterIds = newMasterIds;
					this.masters = newMasters;
					
					// 清理old
					for (ClusterNode clusterNode : oldMasters.values()) {	
						clusterNode.getPhysicalNode().clearConnections("this node exception, automatic reload", true);
					}
					oldMasters.clear();
				} 
			}
			
		} finally {
			availableCheckFlag.set( false );
		}
	}
	
	@Override
	public void heartbeatCheck(long timeout) {	
		
		// 心跳
		for (ClusterNode clusterNode : masters.values()) {			
			
			PhysicalNode physicalNode = clusterNode.getPhysicalNode();
			this.heartbeatCheck( physicalNode, timeout );
		}
	}
	
	// 
	private void heartbeatCheck(PhysicalNode physicalNode, long timeout) {
		
		// 心跳检测, 超时抛弃 
		// --------------------------------------------------------------------------
		long heartbeatTime = TimeUtil.currentTimeMillis() - timeout;		
		long closeTime = TimeUtil.currentTimeMillis() - (timeout * 2);
		
		LinkedList<BackendConnection> heartBeatCons = getNeedHeartbeatCons(physicalNode.conQueue.getCons(), heartbeatTime, closeTime);			
		if ( !heartBeatCons.isEmpty() ) { 			
			for (BackendConnection conn : heartBeatCons) {
				conHeartBeatHanler.doHeartBeat(conn, PING);
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
		
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug( "ClusterHeartbeat: host={}, idle={}, active={}, min={}, max={}, lasttime={}", 
					new Object[] { physicalNode.getHost() + ":" + physicalNode.getPort(),  
					idleCons, activeCons, minCons, maxCons, System.currentTimeMillis() } );

		if ( idleCons > minCons ) {	
			
			if ( idleCons < activeCons ) {
				return;
			}		
			
			//闲置太多
			closeByIdleMany(physicalNode, idleCons - minCons );
			
		} else if ( idleCons < minCons ) {
			
			if ( idleCons > ( minCons * 0.5 ) ) {
				return;
			}
			
			//闲置太少
			if ( (idleCons + activeCons) < maxCons ) {	
				int createCount =  (int)Math.ceil( (minCons - idleCons) / 3F );			
				createByIdleLitte(physicalNode, idleCons, createCount);
			}			
		}
		
	}
	
	private static String getLocalHostQuietly() {
		String localAddress;
		try {
			localAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			localAddress = "localhost";
		}
		return localAddress;
	}
	
	public Map<String, ClusterNode> getMasters() {
		return masters;
	}

	@Override
	public PhysicalNode getPhysicalNode(int id) {
		return null;
	}
    /**
     * 向集群中各master发送ping统计延迟时间
     */
    @Override
    public void latencyCheck() {
    	
    	// CAS， 避免网络不好的情况下，频繁并发的检测
		if (!latencyCheckFlag.compareAndSet(false, true)) {
			return;
		}

		try {
	        
			for (ClusterNode clusterNode : masters.values()) {
	            PhysicalNode physicalNode = clusterNode.getPhysicalNode();
	            this.latencyCheck(physicalNode);
	        }
	        
		} finally {
			latencyCheckFlag.set( false );
		}
    }

    private void latencyCheck(PhysicalNode physicalNode) {
    	JedisConnection conn = null;
        try {
            conn = new JedisConnection(physicalNode.getHost(), physicalNode.getPort(), 3000, 0);
            //
            for( int i =0; i<3; i++) {

        		long time = System.nanoTime();
            	
	            conn.sendCommand(RedisCommand.PING);
	            String value = conn.getBulkReply();
	            if ( value != null && "PONG".equalsIgnoreCase(value) ) {
	            	
	            	PhysicalNode.LatencySample latencySample = new PhysicalNode.LatencySample();
	            	latencySample.time = time;
		            latencySample.latency = (int) (System.nanoTime() - time);
					physicalNode.addLatencySample( latencySample );
	            }
            }
            physicalNode.calculateOverloadByLatencySample( poolCfg.getLatencyThreshold() );

        } catch (JedisConnectionException e) {
        	LOGGER.warn("latency err, host:" + physicalNode.getHost(), e);
            
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
