package com.feeyo.redis.engine.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.loader.zk.ZkClientManage;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.engine.manage.stat.BigLengthCollector.BigLength;
import com.feeyo.redis.engine.manage.stat.CmdAccessCollector.Command;
import com.feeyo.redis.engine.manage.stat.CmdAccessCollector.UserCommand;
import com.feeyo.redis.engine.manage.stat.NetFlowCollector.UserNetFlow;
import com.feeyo.redis.engine.manage.stat.SlowKeyColletor.SlowKey;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.engine.manage.stat.StatUtil.AccessStatInfoResult;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.KafkaPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.RedisStandalonePool;
import com.feeyo.redis.net.backend.pool.cluster.ClusterNode;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.nio.Connection;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.buffer.BufferPool;
import com.feeyo.redis.nio.buffer.bucket.AbstractBucket;
import com.feeyo.redis.nio.buffer.bucket.BucketBufferPool;
import com.feeyo.redis.nio.buffer.page.PageBufferPool;
import com.feeyo.util.JavaUtils;
import com.feeyo.util.ProtoUtils;
import com.feeyo.util.ShellUtils;
import com.feeyo.util.Versions;

/**
 * 自定义后端指令
 * 
 * @author zhuam
 *
 */
public class Manage {
	
	private static Logger LOGGER = LoggerFactory.getLogger( Manage.class );
	private static String JAVA_BIN_PATH = "/usr/local/software/jdk1.7.0_72/bin/";
	
	private static List<String> getOS_JVM_INFO(String cmd) {
		
		List<String> lines = new ArrayList<String>();	
		
		try {
			String output = JavaUtils.launchProcess(cmd, new HashMap<String, String>(), false);
			lines.add(output);
		} catch (IOException e) {
			LOGGER.error("Failed to execute " + cmd, e);
			lines.add("Failed to execute " + cmd);
		} catch (Exception e) {
			LOGGER.error("", e);
			lines.add("Failed to execute " + cmd + ", " + e.getCause());
		}
		return lines;
	}
	
	/**
	 *  支持后端指令
	 *  ----------------------------------------
	 *  USE [POOL_ID]
	 *  
	 *  RELOAD USER
	 *  RELOAD ALL
	 *  RELOAD FRONT
	 *  
	 *  JVM 指令依赖 JAVA_HOME 
	 *  ----------------------------------------
	 *  JVM JSTACK
	 *  JVM JSTAT
	 *  JVM JMAP_HISTO
	 *  JVM JMAP_HEAP
	 * 	JVM PS
	 * 
	 *  SHOW USER
	 *  SHOW USER_NET_IO 
	 *  SHOW CPU
	 *  SHOW MEM
	 *  
	 *  SHOW QPS
	 *  SHOW CONN
	 *  SHOW USER_CONN
	 *  SHOW BUFFER
	 *  
	 *  SHOW BIGKEY
	 *  SHOW BIGLENGTH
	 *  SHOW SLOWKEY
	 *  
	 *  SHOW CMD
	 *  SHOW USER_CMD
	 *  SHOW USER_CMD_DETAIL USER
	 *  SHOW VER
	 *  SHOW NET_IO 该指令兼容过去的 SHOW NETBYTES
	 *  SHOW VM
	 *  SHOW POOL
	 *  SHOW COST
	 *  SHOW USER_DAY_NET_IO
	 *  SHOW POOL_NET_IO POOLNAME
	 *  
	 *  SHOW LOG_ERROR
	 *  SHOW LOG_WARN
	 *  SHOW LOG_INFO
	 *  SHOW LOG_DEBUG
	 *  
	 */
	public static byte[] execute(final RedisRequest request, RedisFrontConnection frontCon) {
		
		int numArgs = request.getNumArgs();
		if ( numArgs != 2 && numArgs != 3 && numArgs != 4 ) {
			return "-ERR Parameter error \r\n".getBytes();
		}
		
		byte[] arg1 = request.getArgs()[0];
		String arg2 = new String( request.getArgs()[1] );
		if ( arg1 == null || arg2 == null) {
			return "-ERR Parameter error \r\n".getBytes();
		}
		
		
		// JVM
		if ( arg1.length == 3 ) {
			
			if ( (arg1[0] == 'J' || arg1[0] == 'j' ) && 
				 (arg1[1] == 'V' || arg1[1] == 'v' ) && 
				 (arg1[2] == 'M' || arg1[2] == 'm' ) ) {
				
				// fake xx
				// /usr/local/software/jdk1.7.0_71/bin/jstack
				StringBuffer cmdBuffer = new StringBuffer();
				if ( JavaUtils.isLinux() )
					cmdBuffer.append( JAVA_BIN_PATH );
				
				// JVM JSTACK
				if ( arg2.equalsIgnoreCase("JSTACK") ) {

					cmdBuffer.append("jstack ").append( JavaUtils.process_pid() );
					return encode( getOS_JVM_INFO( cmdBuffer.toString() ) );
					
				// JVM JSTAT
				} else if ( arg2.equalsIgnoreCase("JSTAT") ) {

					cmdBuffer.append("jstat -gc ").append( JavaUtils.process_pid() );
					return encode( getOS_JVM_INFO( cmdBuffer.toString() ) );
					
				// JVM JMAP_HISTO
				} else if ( arg2.equalsIgnoreCase("JMAP_HISTO") ) {

					cmdBuffer.append("jmap -histo ").append( JavaUtils.process_pid() );
					return encode( getOS_JVM_INFO( cmdBuffer.toString() ) );
					
				// JVM JMAP_HEAP
				} else if ( arg2.equalsIgnoreCase("JMAP_HEAP") ) {
					
					cmdBuffer.append("jmap -heap ").append( JavaUtils.process_pid() );
					return encode( getOS_JVM_INFO( cmdBuffer.toString() ) );
					
				// JVM PS
				} else if ( arg2.equalsIgnoreCase("PS") ) {
					
					String cmd = "ps -mp " + JavaUtils.process_pid() + " -o THREAD,tid,time";
					List<String> line = new ArrayList<String>();
					try {
						line.add(  ShellUtils.execCommand( "bash", "-c", cmd ) );
					} catch (IOException e) {
						line.add( e.getMessage() );
					}
					return encode( line );
				}
			
			// USE ， 支持管理员 use poolId
			} else if (  (arg1[0] == 'U' || arg1[0] == 'u' ) && 
						 (arg1[1] == 'S' || arg1[1] == 's' ) && 
						 (arg1[2] == 'E' || arg1[2] == 'e' ) ) {
				
				try {
					int poolId = Integer.parseInt(arg2);
					AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get(poolId);
					if (pool == null) {
						return "-ERR No such pool. \r\n".getBytes();
					} else {
						int poolType =  pool.getType();
						frontCon.getUserCfg().setUsePool(poolId, poolType);
						
						return "+OK\r\n".getBytes();
					}
				} catch (NumberFormatException e) {
					return "-ERR PoolId is a number. \r\n".getBytes();
				}
			}
			
		// SHOW
		} else if ( arg1.length == 4 ) {
			
			if ( (arg1[0] == 'S' || arg1[0] == 's' ) && 
				 (arg1[1] == 'H' || arg1[1] == 'h' ) && 
				 (arg1[2] == 'O' || arg1[2] == 'o' ) && 
				 (arg1[3] == 'W' || arg1[3] == 'w' ) ) {
				
				
				// SHOW QPS
				if ( arg2.equalsIgnoreCase("QPS") ) {
					
					List<String> lines = new ArrayList<String>();	
					
					AccessStatInfoResult result = StatUtil.getTotalAccessStatInfo().get( StatUtil.STAT_KEY );
					if ( result != null ) {
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append("total=").append( result.totalCount ).append(", ");
						sBuffer.append("slow=").append( result.slowCount ).append(", ");
						sBuffer.append("max=").append( result.maxCount ).append(", ");
						sBuffer.append("min=").append( result.minCount ).append(", ");
						sBuffer.append("avg=").append( result.avgCount ).append(", ");
						sBuffer.append("procTime=").append( result.procTime ).append(", ");
						sBuffer.append("created=").append( result.created );
						
						lines.add( sBuffer.toString() );
					}
					
					return encode( lines );
				
				// SHOW CMD
				} else if ( arg2.equalsIgnoreCase("CMD")  ) {
					
					List<Object> lines = new ArrayList<Object>();		
					
					long sum = 0;
					Set<Entry<String, Command>> entrys = StatUtil.getCommandCountMap().entrySet();
					for (Entry<String, Command> entry : entrys) {	
						Command parent = entry.getValue();
						StringBuffer sBuffer = new StringBuffer();	
						sBuffer.append(  parent.cmd ).append("  ").append( parent.count.get() );
						
						if ( parent.childs != null) {
							List<String> list = new ArrayList<String>();
							list.add( sBuffer.toString() );
							for (Entry<String, Command> childEntry : parent.childs.entrySet()) {
								Command child = childEntry.getValue();
								StringBuffer sb = new StringBuffer();
								sb.append("  ").append( child.cmd ).append("  ").append( child.count.get() );
								list.add( sb.toString() );	
							}
							lines.add( list );
						} else {
							lines.add( sBuffer.toString() );	
						}
						sum += parent.count.get();
					}		
					
					// sum
					StringBuffer sBuffer = new StringBuffer();	
					sBuffer.append( "------" ).append("  ").append( sum );
					lines.add( sBuffer.toString() );	
					
					return encodeObject( lines );
					
				// SHOW USER_CMD
				} else if (arg2.equalsIgnoreCase("USER_CMD")) {

					List<String> lines = new ArrayList<String>();
					
					StringBuffer title = new StringBuffer();
					title.append("USER").append("      ").append("READ").append("      ").append("WRITE").append("      ").append("TOTAL");
					lines.add(title.toString());

					Set<Entry<String, UserCommand>> entrys = StatUtil.getUserCommandCountMap().entrySet();
					for (Entry<String, UserCommand> entry : entrys) {
						UserCommand userCommand = entry.getValue();
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append(userCommand.user).append("  ").append(userCommand.readComandCount.get())
								.append("  ").append(userCommand.writeCommandCount.get()).append("  ")
								.append( userCommand.readComandCount.get() + userCommand.writeCommandCount.get() );
						
						lines.add( sBuffer.toString() );
					}

					return encode(lines);

				// SHOW USER_CMD_DETAIL USER
				} else if ( arg2.equalsIgnoreCase("USER_CMD_DETAIL") && numArgs == 3 ) {
					String user = new String( request.getArgs()[2] );
					
					List<String> lines = new ArrayList<String>();
					
					StringBuffer title = new StringBuffer();
					title.append("USER").append("      ").append("CMD").append("      ").append("COUNT");
					lines.add( title.toString() );

					int sum = 0;
					ConcurrentHashMap<String, UserCommand> userCommandMap = StatUtil.getUserCommandCountMap();
					UserCommand userCommand = userCommandMap.get(user);
					if (userCommand != null) {
						for (Entry<String, AtomicLong> entry : userCommand.commandCount.entrySet()) {
							StringBuffer sBuffer = new StringBuffer();
							sBuffer.append(user).append("  ").append(entry.getKey()).append("  ").append(entry.getValue().get());
							lines.add( sBuffer.toString() );
						}
					}
					
					
					StringBuffer end = new StringBuffer();
					end.append( "------" ).append("  ").append( sum );
					lines.add( end.toString() );

					return encode( lines );
				// SHOW VER
				} else if ( arg2.equalsIgnoreCase("VER") ) {
					
					List<String> lines = new ArrayList<String>();	
					lines.add( Versions.SERVER_VERSION );
					return encode( lines );
				
				// SHOW CPU
				} else if ( arg2.equalsIgnoreCase("CPU") ) {
					
					List<String> lines = new ArrayList<String>();	
					
					if ( JavaUtils.isLinux() ) {
						StringBuffer cmdBuffer = new StringBuffer();
						cmdBuffer.append( "ps -p ").append( JavaUtils.process_pid() ).append(" -o %cpu,%mem" );
						String response;
						try {
							response = ShellUtils.execCommand( "bash", "-c", cmdBuffer.toString() );
							lines.add( response );
						} catch (IOException e) {
							LOGGER.error("get cpu err:", e );
							lines.add( "%CPU %MEM 0 0 " );
						}
						
					} else {
						// %CPU %MEM 231 13.3  兼容
						lines.add( "%CPU %MEM 0 0 " );
					}

					return encode( lines );
					
				// SHOW MEM
				} else if ( arg2.equalsIgnoreCase("MEM") ) {
					
					List<String> lines = new ArrayList<String>();	
					lines.add( "mem:"+ JavaUtils.bytesToString2( Math.round( JavaUtils.getMemUsage() ) ) );
					return encode( lines );
				
				// SHOW CONN
				} else if ( arg2.equalsIgnoreCase("CONN") ) {
					
					int frontSize = 0;
					int backendSize = 0;
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if ( c instanceof RedisFrontConnection ) {
							frontSize++;
						} else {
							backendSize++;
						}
					}

					StringBuffer sBuffer = new StringBuffer();
					sBuffer.append("+");
					sBuffer.append("Connection:");
					sBuffer.append(" front=").append( frontSize ).append(", ");
					sBuffer.append(" backend=").append( backendSize ).append("\r\n");						
					return sBuffer.toString().getBytes();
					
				// SHOW USER_CONN
				} else if ( arg2.equalsIgnoreCase("USER_CONN") ) {
					Map<String, Integer> userMap = new HashMap<String, Integer>();
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if (c instanceof RedisFrontConnection) {
							userMap.put(((RedisFrontConnection) c).getPassword(),
									1 + (userMap.get(((RedisFrontConnection) c).getPassword()) == null ? 0
											: userMap.get(((RedisFrontConnection) c).getPassword())));
						}
					}

					StringBuffer sBuffer = new StringBuffer();
					sBuffer.append("+");
					sBuffer.append("user     front");
					Iterator<Entry<String, Integer>> users = userMap.entrySet().iterator();
					while (users.hasNext()) {
						sBuffer.append("\n");
						Entry<String, Integer> en = users.next();
						sBuffer.append(en.getKey());
						sBuffer.append(" ");
						sBuffer.append(en.getValue());
					}
					sBuffer.append("\r\n");
					
					return sBuffer.toString().getBytes();
					
				// SHOW USER
				} else if ( arg2.equalsIgnoreCase("USER") ) {
					
					// 
					List<String> lines = new ArrayList<String>();							
					ConcurrentHashMap<String, AccessStatInfoResult> results = StatUtil.getTotalAccessStatInfo();
					for (Map.Entry<String, AccessStatInfoResult> entry : results.entrySet()) {		
						
						AccessStatInfoResult result = entry.getValue();		
						
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append("key=").append( result.key ).append(", ");
						sBuffer.append("total=").append( result.totalCount ).append(", ");
						sBuffer.append("slow=").append( result.slowCount ).append(", ");
						sBuffer.append("max=").append( result.maxCount ).append(", ");
						sBuffer.append("min=").append( result.minCount ).append(", ");
						sBuffer.append("avg=").append( result.avgCount ).append(", ");
						sBuffer.append("procTime=").append( result.procTime ).append(", ");
						
						sBuffer.append("netIn (");
						sBuffer.append("max=").append(  result.netInBytes[1]  ).append(", ");
						sBuffer.append("min=").append(  result.netInBytes[2]  ).append(", ");
						sBuffer.append("avg=").append(  result.netInBytes[3]  ).append(") ").append(", ");
						
						sBuffer.append("netOut (");
						sBuffer.append("max=").append(  result.netOutBytes[1]  ).append(", ");
						sBuffer.append("min=").append(  result.netOutBytes[2]  ).append(", ");
						sBuffer.append("avg=").append(  result.netOutBytes[3]  ).append(") ").append(", ");
						
						sBuffer.append("created=").append( result.created );
						
						lines.add( sBuffer.toString() );
					}
					
					return encode( lines );
					
				// SHOW FRONT
				} else if ( arg2.equalsIgnoreCase("FRONT") ) {
					
					List<String> lines = new ArrayList<String>();
					
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if ( c instanceof RedisFrontConnection ) {
							lines.add( c.toString() );
						}
					}
					return encode( lines );
				
				// SHOW BUFFER
				} else if ( arg2.equalsIgnoreCase("BUFFER") ) {
					
					BufferPool bufferPool = NetSystem.getInstance().getBufferPool();
					long usedBufferSize = bufferPool.getUsedBufferSize().get();
					long maxBufferSize = bufferPool.getMaxBufferSize();
					long minBufferSize = bufferPool.getMinBufferSize();
					
					long sharedOptsCount = bufferPool.getSharedOptsCount();
					
					int capacity = 0;
					
					if ( bufferPool instanceof BucketBufferPool ) {
						BucketBufferPool p = (BucketBufferPool) bufferPool;
						AbstractBucket[] buckets = p.buckets();
						for (AbstractBucket b : buckets) {
							capacity += b.getCount();
						}
						
						int bucketLen = buckets.length;
						
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append("+");
						sBuffer.append("Buffer:");
						sBuffer.append(" capacity=").append( capacity ).append(",");
						sBuffer.append(" minBufferSize=").append( minBufferSize ).append(",");
						sBuffer.append(" maxBufferSize=").append( maxBufferSize ).append(",");
						sBuffer.append(" usedBufferSize=").append( usedBufferSize ).append(",");
						sBuffer.append(" buckets=").append( bucketLen ).append(",");
						sBuffer.append(" shared=").append( sharedOptsCount ).append("\r\n");
						
						return sBuffer.toString().getBytes();
						
					} else if ( bufferPool instanceof PageBufferPool ) {
						
						List<String> lines = new ArrayList<String>();
						
						ConcurrentHashMap<Long, Long> bufferpoolUsageMap = bufferPool.getNetDirectMemoryUsage();
						
						// 网络packet处理，在buffer pool 已经使用DirectMemory内存
						long usedforNetwork = 0;
						for (Map.Entry<Long, Long> entry : bufferpoolUsageMap.entrySet()) {
		                    long value = entry.getValue() ;
							lines.add("threadId=" + entry.getKey() + ", value=" + ( value > 0 ? JavaUtils.bytesToString2(value) : "0") );
							usedforNetwork = usedforNetwork + value;
						}
						
						lines.add( "minBufferSize=" + JavaUtils.bytesToString2( minBufferSize ) );
						lines.add( "maxBufferSize=" + JavaUtils.bytesToString2( maxBufferSize ) );
						lines.add( "usedBufferSize=" + JavaUtils.bytesToString2( usedforNetwork ) );

						return encode( lines );
					}
					
				// SHOW BUCKET
				} else if ( arg2.equalsIgnoreCase("BUCKET") ) {
					
					List<String> lines = new ArrayList<String>();	
					
					BufferPool bufferPool = NetSystem.getInstance().getBufferPool();
					if ( bufferPool instanceof BucketBufferPool ) {
						BucketBufferPool p = (BucketBufferPool) bufferPool;
						AbstractBucket[] buckets = p.buckets();

						for(AbstractBucket b: buckets) {
							StringBuffer sBuffer = new StringBuffer();
							sBuffer.append(" chunkSize=").append( b.getChunkSize() ).append(",");
							sBuffer.append(" queueSize=").append( b.getQueueSize() ).append( ", " );
							sBuffer.append(" count=").append( b.getCount() ).append( ", " );
							sBuffer.append(" useCount=").append( b.getUsedCount() ).append( ", " );
							sBuffer.append(" shared=").append( b.getShared() );		
							lines.add( sBuffer.toString()  );
						}		
					}
					return encode( lines );
					
				// SHOW BIGKEY
				} else if ( arg2.equalsIgnoreCase("BIGKEY") ) {
					
					List<String> lines = new ArrayList<String>();						
					for(BigKey bigkey: StatUtil.getBigKeys()) {
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append( bigkey.cmd ).append("  ");
						sBuffer.append( bigkey.key ).append( "  " );
						sBuffer.append( bigkey.size ).append( "  " );
						sBuffer.append( bigkey.count.get() );
						lines.add( sBuffer.toString()  );
					}			
					return encode( lines );
					
				// SHOW BACKEND
				} else if ( arg2.equalsIgnoreCase("BACKEND") ) {
					
					List<String> lines = new ArrayList<String>();
					
					Map<String, AtomicInteger> poolConnections = new HashMap<String, AtomicInteger>();
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if ( c instanceof RedisBackendConnection ) {
							// 统计每个redis池的连接数 
							String poolName = ((RedisBackendConnection) c).getPhysicalNode().getPoolName();
							AtomicInteger poolConnCount = poolConnections.get(poolName);
							if ( poolConnCount == null ) {
								poolConnections.put(poolName, new AtomicInteger(1));
							} else {
								poolConnCount.incrementAndGet();
							}
							
							lines.add( c.toString() );
						}
					}
					
					StringBuffer sb = new StringBuffer();
					for (Map.Entry<String, AtomicInteger> entry : poolConnections.entrySet()) {
						sb.append(entry.getKey()).append(":").append(entry.getValue().get()).append(". ");
					}
					lines.add(sb.toString());
					
					return encode( lines );
				
				// SHOW POOL_NET_IO POOLNAME
				} else if ( arg2.equalsIgnoreCase("POOL_NET_IO")  && numArgs == 3 ) {
					
					// TODO
					String poolName = new String( request.getArgs()[2] );
					
					List<String> lines = new ArrayList<String>();
					
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					long minStartupTime = -1;
					long maxLastLargeMessageTime = -1;
					long totalLargeCount = 0;
					long totalNetInCount = 0;
					long totalNetInBytes = 0;
					long totalNetOutBytes = 0;
					
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if ( c instanceof RedisBackendConnection ) {
							// 统计每个redis池的连接数 
							if (((RedisBackendConnection) c).getPhysicalNode().getPoolName().equals(poolName)) {
								StringBuffer sb = new StringBuffer();
								sb.append("ID=").append(c.getId()).append(". ");
								sb.append("StartupTime=").append(c.getStartupTime()).append(". ");
								sb.append("LastLargeMessageTime=").append(c.getLastLargeMessageTime()).append(". ");
								sb.append("LargeCount=").append(c.getLargeCount()).append(". ");
								sb.append("NetInCount=").append(c.getNetInCount()).append(". ");
								sb.append("NetInBytes=").append(c.getNetInBytes()).append(". ");
								sb.append("NetOutBytes=").append(c.getNetOutBytes()).append(". ");
								lines.add( sb.toString() );
								
								minStartupTime = minStartupTime < 0 ? c.getStartupTime() : Math.min(minStartupTime, c.getStartupTime());
								maxLastLargeMessageTime = Math.max(maxLastLargeMessageTime, c.getLastLargeMessageTime());
								totalLargeCount = totalLargeCount + c.getLargeCount();
								totalNetInCount = totalNetInCount + c.getNetInCount();
								totalNetInBytes = totalNetInBytes + c.getNetInBytes();
								totalNetOutBytes = totalNetOutBytes + c.getNetOutBytes();
							}
						}
					}
					StringBuffer end = new StringBuffer();
					end.append("MinStartupTime=").append(minStartupTime).append(". ");
					end.append("MaxLastLargeMessageTime=").append(maxLastLargeMessageTime).append(". ");
					end.append("TotalLargeCount=").append(totalLargeCount).append(". ");
					end.append("TotalNetInCount=").append(totalNetInCount).append(". ");
					end.append("TotalNetInBytes=").append(totalNetInBytes).append(". ");
					end.append("TotalNetOutBytes=").append(totalNetOutBytes).append(". ");
					lines.add(end.toString());
					
					return encode( lines );
					
				// SHOW NETBYTES/NET_IO
				} else if ( arg2.equalsIgnoreCase("NETBYTES")  || arg2.equalsIgnoreCase("NET_IO")) {
					
					List<String> lines = new ArrayList<String>();		
					
					AccessStatInfoResult result = StatUtil.getTotalAccessStatInfo().get( StatUtil.STAT_KEY );
					if ( result != null ) {
						
						StringBuffer line0 = new StringBuffer();
						line0.append( "NetIn/NetOut" ).append(",  ");
						line0.append( "total" ).append(",  ");
						line0.append( "max" ).append(",  ");
						line0.append( "min" ).append(",  ");
						line0.append( "avg" );
						
						StringBuffer line1 = new StringBuffer();
						line1.append( "NetIn" ).append(", ");
						line1.append( result.netInBytes[0] ).append(", ");
						line1.append( result.netInBytes[1] ).append(", ");
						line1.append( result.netInBytes[2] ).append(", ");
						line1.append( result.netInBytes[3] );
						
						StringBuffer line2 = new StringBuffer();
						line2.append( "NetOut" ).append(", ");
						line2.append( result.netOutBytes[0] ).append(", ");
						line2.append( result.netOutBytes[1] ).append(", ");
						line2.append( result.netOutBytes[2] ).append(", ");
						line2.append( result.netOutBytes[3] );
						
						StringBuffer line3 = new StringBuffer();
						line3.append( result.created );
						
						lines.add( line0.toString() );
						lines.add( line1.toString() );
						lines.add( line2.toString() );
						lines.add( line3.toString() );
					}
					
					return encode( lines );
					
				// SHOW USER_NET_IO
				} else if ( arg2.equalsIgnoreCase("USER_NET_IO") ) {
					
					List<String> lines = new ArrayList<String>();
					StringBuffer titleLine = new StringBuffer();
					titleLine.append("User").append(",  ");
					titleLine.append("NetIn/NetOut").append(",  ");
					titleLine.append("total").append(",  ");
					titleLine.append("max").append(",  ");
					titleLine.append("min").append(",  ");
					titleLine.append("avg");
					lines.add(titleLine.toString());
					
					for (Map.Entry<String, AccessStatInfoResult> entry : StatUtil.getTotalAccessStatInfo().entrySet()) { 
						if (!StatUtil.STAT_KEY.equals(entry.getKey())) {
							AccessStatInfoResult result = entry.getValue();

							StringBuffer line1 = new StringBuffer();
							line1.append(result.key).append(", ");
							line1.append("NetIn").append(", ");
							line1.append(result.netInBytes[0]).append(", ");
							line1.append(result.netInBytes[1]).append(", ");
							line1.append(result.netInBytes[2]).append(", ");
							line1.append(result.netInBytes[3]);

							StringBuffer line2 = new StringBuffer();
							line2.append(result.key).append(", ");
							line2.append("NetOut").append(", ");
							line2.append(result.netOutBytes[0]).append(", ");
							line2.append(result.netOutBytes[1]).append(", ");
							line2.append(result.netOutBytes[2]).append(", ");
							line2.append(result.netOutBytes[3]);

							StringBuffer line3 = new StringBuffer();
							line3.append(result.created);

							lines.add(line1.toString());
							lines.add(line2.toString());
							lines.add(line3.toString());
						
						}
					}
					return encode(lines);
				
				//SHOW USER_DAY_NET_IO
				} else if ( arg2.equalsIgnoreCase("USER_DAY_NET_IO") ) {
					
					List<String> lines = new ArrayList<String>();
					StringBuffer titleLine = new StringBuffer();
					titleLine.append("User").append("         ");
					titleLine.append("NetIn").append("         ");
					titleLine.append("NetOut");
					lines.add(titleLine.toString());
					
					long totalNetIn = 0;
					long totalNetOut = 0;
					for (Map.Entry<String, UserNetFlow> entry : StatUtil.getUserFlowMap().entrySet()) { 
						if (!StatUtil.STAT_KEY.equals(entry.getKey())) {
							StringBuffer sb = new StringBuffer();
							UserNetFlow userNetIo = entry.getValue();
							sb.append(userNetIo.password).append("  ");
							sb.append( JavaUtils.bytesToString2( userNetIo.netIn.get() ) ).append("  ");
							sb.append( JavaUtils.bytesToString2( userNetIo.netOut.get() ) );
							totalNetIn = totalNetIn + userNetIo.netIn.get();
							totalNetOut = totalNetOut + userNetIo.netOut.get();
							
							lines.add(sb.toString());
						}
					}
					
					StringBuffer total = new StringBuffer();
					total.append("total").append("    ");
					total.append( JavaUtils.bytesToString2(totalNetIn) ).append("    ");
					total.append( JavaUtils.bytesToString2(totalNetOut) );
					
					lines.add(total.toString());
					return encode(lines);
					
				//	SHOW LOG_ERROR
				}  else if ( arg2.equalsIgnoreCase("LOG_ERROR") ) {
					List<String> lines = showLog(request, "error.log");
					
					return encode2( lines );
					
				// SHOW LOG_WARN
				} else if ( arg2.equalsIgnoreCase("LOG_WARN") ) {
					List<String> lines = showLog(request, "warn.log");
					
					return encode2( lines );
					
				// SHOW LOG_INFO
				} else if ( arg2.equalsIgnoreCase("LOG_INFO") ) {
					List<String> lines = showLog(request, "info.log");
					
					return encode2( lines );
					
				// SHOW LOG_DEBUG
				} else if ( arg2.equalsIgnoreCase("LOG_DEBUG") ) {
					List<String> lines = showLog(request, "debug.log");
					
					return encode2( lines );
					
				// SHOW VM
				} else if ( arg2.equalsIgnoreCase("VM") ) {
				    List<String> ret = new ArrayList<String>();
                    try {
                    	String cmd1 = ShellUtils.osType == ShellUtils.OSType.OS_TYPE_MAC ? "iostat" : "iostat -x";
                    	String cmd2 = ShellUtils.osType == ShellUtils.OSType.OS_TYPE_MAC ? "vm_stat": "vmstat";
                    	
                        String iostatOut = ShellUtils.execCommand( "bash", "-c", cmd1 );
                        String vmstatOut = ShellUtils.execCommand( "bash", "-c", cmd2 );
                        StringBuilder sb = new StringBuilder();
                        sb.append(iostatOut);
                        sb.append(" \n");
                        sb.append("-------------------------------------VMSTAT-------------------------------------\n");
                        sb.append(vmstatOut);

                        String[] lines = sb.toString().split("\\n");
                        ret.add("-------------------------------------IOSTAT-------------------------------------");
                        for (int i=1; i<lines.length; i++) {
                            if (lines[i].equals(""))
                                continue;
                            ret.add(lines[i]);
                        }

                    } catch (IOException e) {
                        LOGGER.error("get vm err:", e );
                        ret.add( "SHOW VM ERROR " );
                    }

                    return encode2(ret);
                    
                // SHOW POOL
				} else if ( arg2.equalsIgnoreCase("POOL") ) {
					
					List<Object> list = new ArrayList<Object>();
					
					Map<Integer, AbstractPool> pools = RedisEngineCtx.INSTANCE().getPoolMap();
					StringBuffer titleLine = new StringBuffer();
					titleLine.append("  PoolId").append("   ");
					titleLine.append("PoolName").append("    ");
					titleLine.append("PoolType").append("    ");
					titleLine.append("Address").append("               ");
					titleLine.append("MinCom").append("    ");
					titleLine.append("MaxCon").append("   ");
					titleLine.append("IdlCon").append("   ");
					titleLine.append("ActiveCon").append("   ");
					titleLine.append("ClusterNodeState");
					list.add(titleLine.toString());
					
					for(AbstractPool pool : pools.values() ) {
						if ( pool instanceof RedisStandalonePool ) {
							StringBuffer sb = new StringBuffer();
							RedisStandalonePool redisStandalonePool = (RedisStandalonePool) pool;
							PhysicalNode physicalNode = redisStandalonePool.getPhysicalNode();
							if (physicalNode == null) 
								continue;
								
							sb.append("   ");
							sb.append(redisStandalonePool.getId()).append("       ");
							sb.append(physicalNode.getPoolName()).append("       ");
							sb.append("Standalone").append("  ");
							sb.append(physicalNode.getName()).append("       ");
							sb.append(physicalNode.getMinCon()).append("       ");
							sb.append(physicalNode.getMaxCon()).append("       ");
							sb.append(physicalNode.getIdleCount()).append("          ");
							sb.append(physicalNode.getActiveCount());
							list.add(sb.toString());
							
						} else if ( pool instanceof RedisClusterPool ) {
							RedisClusterPool redisClusterPool = (RedisClusterPool) pool;
							Map<String, ClusterNode> masters = redisClusterPool.getMasters();
							List<String> clusterInfo = new ArrayList<String>();
							for (ClusterNode clusterNode : masters.values()) {
								PhysicalNode physicalNode = clusterNode.getPhysicalNode(); 
								StringBuffer sb = new StringBuffer();
								sb.append(redisClusterPool.getId()).append("       ");
								sb.append(physicalNode.getPoolName()).append("       ");
								sb.append("cluster").append("     ");
								sb.append(physicalNode.getName()).append("       ");
								sb.append(physicalNode.getMinCon()).append("       ");
								sb.append(physicalNode.getMaxCon()).append("       ");
								sb.append(physicalNode.getIdleCount()).append("       ");
								sb.append(physicalNode.getActiveCount()).append("          ");;
								sb.append(!clusterNode.isFail());
								clusterInfo.add(sb.toString());
								sb.append(clusterNode.getConnectInfo());
							}
							list.add(clusterInfo);
						} else if (pool instanceof KafkaPool) {
							// TODO 待完善
						}
					}
					return encodeObject(list);

				// SHOW COST
				} else if (arg2.equalsIgnoreCase("COST")) {
					Collection<Entry<String, AtomicLong>> entrys = StatUtil.getCommandProcTimeMap().entrySet();

					List<String> lines = new ArrayList<String>();
					for (Entry<String, AtomicLong> entry : entrys) {
						StringBuffer sBuffer = new StringBuffer();
						sBuffer.append(entry.getKey()).append(": ").append(entry.getValue().get());
						lines.add(sBuffer.toString());
					}
					return encode(lines);
				
				// SHOW BIGLENGTH
				} else  if(arg2.equalsIgnoreCase("BIGLENGTH")) {
					List<String> lines = new ArrayList<String>();
					StringBuffer titleLine = new StringBuffer();
					titleLine.append("cmd").append(",  ");
					titleLine.append("key").append(",  ");
					titleLine.append("length").append(",  ");
					titleLine.append("count_1k").append(",  ");
					titleLine.append("count_10k");
					lines.add(titleLine.toString());
					for (BigLength bigLength : StatUtil.getBigLengthMap().values()) { 
						StringBuffer line1 = new StringBuffer();
						line1.append(bigLength.cmd).append(", ");
						line1.append(bigLength.key).append(", ");
						line1.append(bigLength.length.get()).append(", ");
						line1.append(bigLength.count_1k.get()).append(", ");
						line1.append(bigLength.count_10k.get());
						lines.add(line1.toString());
					}
					return encode(lines);
				
				// SHOW BIGLENGTH
				} else if (arg2.equalsIgnoreCase("SLOWKEY")) {
					List<String> lines = new ArrayList<String>();
					StringBuffer titleLine = new StringBuffer();
					titleLine.append("cmd").append(",  ");
					titleLine.append("key").append(",  ");
					titleLine.append("count");
					lines.add(titleLine.toString());
					for (SlowKey slowKey : StatUtil.getSlowKey()) {
						StringBuffer line1 = new StringBuffer();
						line1.append(slowKey.cmd).append(", ");
						line1.append(slowKey.key).append(", ");
						line1.append(slowKey.count);
						lines.add(line1.toString());
					}
					return encode(lines);
				}
				
			// NODE
			} else if (  (arg1[0] == 'N' || arg1[0] == 'n' ) && 
						 (arg1[1] == 'O' || arg1[1] == 'o' ) && 
						 (arg1[2] == 'D' || arg1[2] == 'd' ) && 
						 (arg1[3] == 'E' || arg1[3] == 'e' ) ) {
				
				// TODO 此处有一些乱, 暂时屏蔽
				// 接口应该这样设计， XX 服务， XX 方法， 输入-异步回调  或 输入-输出， 内部逻辑的过程 交由 XX服务来实现
				/*
				NodeManageService nodeManageService = new NodeManageService(frontCon);
				try {
					NodeManageRequest nodeManageRequest = nodeManageService.requestIntegration(request);
					return nodeManageService.excute(nodeManageRequest);
				} catch (NodeManageParamException e) {
					StringBuffer sb = new StringBuffer();
					sb.append("-ERR ");
					sb.append(e.getMessage());
					sb.append("\r\n");
					return sb.toString().getBytes();
				}
				*/
				return "-ERR not support \r\n".getBytes();
				
			}

		// RELOAD
		} else if ( arg1.length == 6 ) {
			
			
			if ( (arg1[0] == 'R' || arg1[0] == 'r' ) && 
				 (arg1[1] == 'E' || arg1[1] == 'e' ) && 
				 (arg1[2] == 'L' || arg1[2] == 'l' ) && 
				 (arg1[3] == 'O' || arg1[3] == 'o' ) &&
				 (arg1[4] == 'A' || arg1[4] == 'a' ) &&
				 (arg1[5] == 'D' || arg1[5] == 'd' ) ) {
				
				// reload all
				if ( arg2.equalsIgnoreCase("ALL") ) {
					byte[] buff = RedisEngineCtx.INSTANCE().reloadAll();
					return buff;
					
				// reload user
				} else if ( arg2.equalsIgnoreCase("USER") ) {
					byte[] buff = RedisEngineCtx.INSTANCE().reloadUser();
					return buff;
				
				// reload front
				} else if ( arg2.equalsIgnoreCase("FRONT") ) {
					
					ConcurrentMap<Long, Connection> allConnections = NetSystem.getInstance().getAllConnectios();
					Iterator<Entry<Long, Connection>> it = allConnections.entrySet().iterator();
					while (it.hasNext()) {
						Connection c = it.next().getValue();
						if ( c instanceof RedisFrontConnection ) {
							LOGGER.info("close: {}", c);
							c.close("manage close");
						}
					}
					
					return "+OK\r\n".getBytes();
				} else if ( arg2.equalsIgnoreCase("PATH") ) {
					
					JAVA_BIN_PATH = new String( request.getArgs()[2] );
					
					return "+OK\r\n".getBytes();
				}
			}

		// ZK upload / activation
		} else if (arg1.length == 2) {
			if ( (arg1[0] == 'Z' || arg1[0] == 'z' ) &&
				 (arg1[1] == 'K' || arg1[1] == 'k' ) ) {
				return ZkClientManage.execute(request);
			}
			
		// cluster 
		} else if (arg1.length == 7) {
			if ( (arg1[0] == 'C' || arg1[0] == 'c' ) && 
				 (arg1[1] == 'L' || arg1[1] == 'l' ) && 
				 (arg1[2] == 'U' || arg1[2] == 'u' ) && 
				 (arg1[3] == 'S' || arg1[3] == 's' ) &&
				 (arg1[4] == 'T' || arg1[4] == 't' ) &&
				 (arg1[5] == 'E' || arg1[5] == 'e' ) &&
				 (arg1[6] == 'R' || arg1[6] == 'r' ) ) {
			
				// 随机获取
				AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( frontCon.getUserCfg().getPoolId() );
				if ( pool.getType() != 1 ) {
					return "-ERR Not cluster pool. \r\n".getBytes();
				}
				
				PhysicalNode pysicalNode = ((RedisClusterPool) pool).getPhysicalNodeBySlot(0);
				if ( pysicalNode == null ) {
					return "-ERR node unavailable. \r\n".getBytes();
				}
				
				try {
				
					RedisBackendConnection backendCon = pysicalNode.getConnection(new DirectTransTofrontCallBack(), frontCon);
					if (backendCon == null) {
						frontCon.writeErrMessage("not idle backend connection, pls wait !!!");
					} else {
						backendCon.write( request.encode() );
					}
					
					return null;	// null, not write
				} catch (IOException e) {
					LOGGER.error("", e);
				}
			}
			
		}
		return "-ERR Not supported. \r\n".getBytes();
	}
	
	public static synchronized byte[] encode2(List<String> lines) {
		StringBuffer sb = new StringBuffer();
		if (lines == null || lines.size() <= 0) {
			sb.append("-ERR no data.\r\n");
		} else {
			sb.append("+");
			for (String line : lines) {
				sb.append(line);
				sb.append("\n");
			}
			sb.append("\r\n");
		}
		return sb.toString().getBytes();
	}

	public static synchronized byte[] encode(List<String> lines) throws BufferOverflowException {
		
		// 粗略分配
		int bufferSize = 0;
		for(String line: lines) {
			bufferSize += line.getBytes().length + 12;
		}
		
		ByteBuffer buffer = ByteBuffer.allocate( bufferSize );
		if ( lines.size() == 1 ) {			
			buffer.put( (byte)'+' );
			buffer.put( lines.get(0).getBytes() );
			buffer.put( "\r\n".getBytes() );
			
		} else if ( lines.size() > 1 ) {			
			
			buffer.put( (byte)'*' );
			buffer.put(  ProtoUtils.convertIntToByteArray( lines.size() ) );
			buffer.put( "\r\n".getBytes() );
			
			for(int i = 0; i < lines.size(); i++) {
				byte[] lineBytes = lines.get(i).getBytes();
				buffer.put( (byte)'$' );
				buffer.put(  ProtoUtils.convertIntToByteArray( lineBytes.length ) );
				buffer.put( "\r\n".getBytes() );
				buffer.put( lineBytes );
				buffer.put( "\r\n".getBytes() );				
			}
			
		} else {
			return "-ERR no data.\r\n".getBytes();
		}
		
		buffer.flip();		
		byte[] data = new byte[ buffer.remaining() ];
		buffer.get(data);
		return data;
	}
	
	@SuppressWarnings("unchecked")
	public static synchronized byte[] encodeObject(List<Object> lines) throws BufferOverflowException {
		// 粗略分配
		int bufferSize = getSize(lines);
		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		
		if (lines.size() > 0) {
			buffer.put((byte) '*');
			buffer.put(ProtoUtils.convertIntToByteArray(lines.size()));
			buffer.put("\r\n".getBytes());

			for (int i = 0; i < lines.size(); i++) {
				Object obj = lines.get(i);
				if (obj instanceof String) {
					byte[] lineBytes = String.valueOf(obj).getBytes();
					buffer.put((byte) '$');
					buffer.put(ProtoUtils.convertIntToByteArray(lineBytes.length));
					buffer.put("\r\n".getBytes());
					buffer.put(lineBytes);
					buffer.put("\r\n".getBytes());
					
				} else if (obj instanceof List) {
					buffer.put( encodeObject( (List<Object>) obj ) );
				}
			}

		} else {
			return "-ERR no data.\r\n".getBytes();
		}

		buffer.flip();
		byte[] data = new byte[buffer.remaining()];
		buffer.get(data);
		return data;
	}
	
	@SuppressWarnings("unchecked")
	private static int getSize(List<Object> lines) {
		// 粗略分配
		int bufferSize = 0;
		for (Object line : lines) {
			if (line instanceof String) {
				bufferSize += String.valueOf(line).getBytes().length + 12;
			} else if (line instanceof List) {
				bufferSize += getSize( (List<Object>)line );
			}
		}
		return bufferSize;
	}
	
	private static List<String> showLog(RedisRequest request, String fileName) {
		long defaultLength = 1024;
		
		StringBuffer logDir = new StringBuffer();
		logDir.append( System.getProperty("log4jHome") );
		logDir.append( File.separator );
		logDir.append( "logs" );

		List<String> lines = new ArrayList<String>();	
		
		if (request.getNumArgs() == 3) {
			defaultLength = Long.parseLong(new String (request.getArgs()[2])) * 1024;
		}
		getLine( logDir.toString(), fileName, defaultLength, lines);
		
		return lines;
	}
	
	
	private static void getLine(String path, String fileName, long length, List<String> list) {
		RandomAccessFile rf = null;
		try {
			String line;
			rf = new RandomAccessFile(path + File.separator + fileName, "r");

			long fileLength = rf.length();

			if (fileLength < length) {
				String prevFileName;
				if (fileName.indexOf("log") == fileName.length() - 3) {
					prevFileName = fileName + ".1";
					getLine(path, prevFileName, length - fileLength, list);
				} else {
					int index = Integer.parseInt(fileName.substring(fileName.length() - 1)) + 1;
					if (index <= 5) {
						prevFileName = fileName.substring(0, fileName.length() - 1) + index;
						getLine(path, prevFileName, length - fileLength, list);
					}
				}
				length = fileLength;
			}

			if (fileLength == length) {
				while ((line = rf.readLine()) != null) {
					list.add(line);
				}
			} else {
				long next = fileLength - length - 1;
				rf.seek(next);
				int c = -1;
				while (true) {
					if (next == 0) {
						break;
					}
					c = rf.read();
					if ((c == '\n' || c == '\r')) {
						break;
					}
					next--;
					rf.seek(next);
				}

				while ((line = rf.readLine()) != null) {
					list.add(line);
				}
			}
		} catch (IOException e) {

		} finally {
			if (rf != null ) {
				try {
					rf.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
