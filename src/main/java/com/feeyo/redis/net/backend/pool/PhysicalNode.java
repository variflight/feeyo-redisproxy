package com.feeyo.redis.net.backend.pool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.BackendConnectionFactory;
import com.feeyo.redis.net.backend.callback.BackendCallback;


/**
 * 后端物理节点的 connection 连接池
 * 
 * @author zhuam
 *
 */
public class PhysicalNode {
	
	private static Logger LOGGER = LoggerFactory.getLogger( PhysicalNode.class );

	protected final String name;
	protected final int size;
	
	// TODO: 待优化
	public final ConQueue conQueue = new ConQueue();
	
	protected final int poolType;
	protected final String poolName;
	
	protected String host;
	protected int port;
	protected int minCon;
	protected int maxCon;
	
    // 
	private volatile boolean isOverload = false;
    private LatencyTimeSeries latencyTimeSeries = new LatencyTimeSeries();

	protected final BackendConnectionFactory factory;
	
	public PhysicalNode(BackendConnectionFactory factory, int poolType, String poolName, 
			int minCon, int maxCon, String host, int port) {
		
		this.factory = factory;
		this.poolType = poolType;
		this.poolName = poolName;
		
		this.host = host;
		this.port = port;
		this.minCon = minCon;
		this.maxCon = maxCon;
		
		this.size = maxCon;
		this.name = host + ":" + port;
	}
	
	// 新建连接，异步填充后端连接池
	public void createNewConnection() throws IOException {	
		
		createNewConnection(new BackendCallback() {
			@Override
			public void connectionAcquired(BackendConnection conn) {
				conQueue.getCons().add( conn ); 
			}	
			
			public void connectionClose(BackendConnection conn, String reason) {
				conQueue.getCons().remove( conn );  
			}

			@Override
			public void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException {
				//ignore
			}

			@Override
			public void connectionError(Exception e, BackendConnection conn) {
				//ignore
			}
		}, null);
	}
	
	public BackendConnection createNewConnection(BackendCallback callback, Object attachment) throws IOException {
		
		 int activeCons = this.getActiveCount();// 当前最大活动连接
         if ( activeCons + 1 > size ) {// 下一个连接大于最大连接数
         		LOGGER.error("PN={} the max activeConns={} size can not be max than maxConns={}", new Object[]{ name, (activeCons+1), size } );
             	throw new IOException("the max activeConnnections size can not be max than maxconnections");                
         } else {      
         	
         	if ( LOGGER.isDebugEnabled() ) 
         		LOGGER.debug( " no ilde connection in pool, create new connection for " + this.name + " of " + poolName);           
             
         	// create connection
        	BackendConnection con = factory.make(this, callback, attachment);
    		con.setLastTime( TimeUtil.currentTimeMillis() );
    		return con;
         }
	}
	
	public boolean initConnections() {
		
        int initSize = minCon;
        LOGGER.info("init redis node ,create connections total " + initSize + " for " + host + ":" + port);
		for (int i = 0; i < initSize; i++) {
			try {
				this.createNewConnection();				
			} catch (Exception e) {
				LOGGER.error(" init connection error.", e);
			}
		}
		
		LOGGER.info("init redis node finished");
        return true;
    }
	
	
	public int getActiveCount() {
        return this.conQueue.getActiveCountForNode(this);
    }
	
	public int getIdleCount() {
		return this.conQueue.getCons().size();
	}

    public void clearConnections(String reason, boolean isForce) {    	
    	LOGGER.info("{} node cleanup, reason={}, force={}", new Object[] { this.name, reason, isForce });    	
    	if ( isForce ) {
    		this.conQueue.clearConnections(reason, this);
    		
    	} else {
        	long idleTimeout = 1000 * 45;  //45s 超时 
    		this.conQueue.setIdleTimeConnections(this, idleTimeout);
    	}
    }

    public BackendConnection getConnection(BackendCallback callback, Object attachment)
            throws IOException {
    	
    	BackendConnection con = this.conQueue.takeIdleCon();
        if (con != null) {
        	con.setAttachement( attachment );
        	con.setCallback( callback );        	
        	con.setBorrowed(true);
        	con.setLastTime( TimeUtil.currentTimeMillis() ); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致执行失败
        	return con;
        } 
        
        return null;
    }
	
	public void releaseConnection(BackendConnection c) {
        
		c.setBorrowed( false );
        c.setAttachement( null );
        c.setCallback( null );
        c.setLastTime( TimeUtil.currentTimeMillis() );     
        
        ConQueue queue = this.conQueue;
        boolean ok = false;
        ok = queue.getCons().offer(c);
        if ( !ok ) {
        	LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
        
        if ( LOGGER.isDebugEnabled() ) {
        	LOGGER.debug("release channel " + c);
        }
    }
	
	public void removeConnection(BackendConnection conn) {
		
		ConQueue queue = this.conQueue;
		if (queue != null) {
			queue.removeCon(conn);
		}
	}	
	
	// 节点的负载
    public boolean isOverload() {
        return this.isOverload;
    }

    //
    public void addLatencySample(LatencySample sample) {
    	this.latencyTimeSeries.addSample(sample);
    }
    
    public void calculateOverloadByLatencySample(float latencyThreshold) {
    	//
    	boolean newOverload = false;
    	//
    	int latency = this.latencyTimeSeries.calculateLatency();
		if ( latency != -1 )
			newOverload = (latency >= ( latencyThreshold * 1000000 ) ); // 配置中采用毫秒单位， 此处转为纳秒
		//
		if ( newOverload != this.isOverload )
			LOGGER.warn("physicalNode overload value changed, host={}, port={}, isOverload={}/{} latencyThreshold={} latency={}",
					new Object[]{ this.host , this.port, this.isOverload, newOverload, latencyThreshold, latency } );
		//
		this.isOverload = newOverload;
    }
	
    public List<LatencySample> getLatencySamples() {
    	return this.latencyTimeSeries.getSparseSamples();
    }
    
	
	public String getName() {
		return name;
	}

	public int getPoolType() {
		return poolType;
	}

	public String getPoolName() {
		return poolName;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public int getMinCon() {
		return minCon;
	}

	public int getMaxCon() {
		return maxCon;
	}	
	
	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}	
	

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(" ( ");
		sb.append("name=").append(name).append(", ");
		sb.append("size=").append(size).append(", ");
		sb.append("poolType=").append(poolType).append(", ");
		sb.append("poolName=").append(poolName).append(", ");
		sb.append("host=").append(host).append(", ");
		sb.append("port=").append(port).append(", ");
		sb.append("minCon=").append(minCon).append(", ");
		sb.append("maxCon=").append(maxCon).append(" ) ");
		return sb.toString();
	}
	
	
	//
	//
	public static class LatencyTimeSeries {
		
		private static final int SAMPLE_SIZE = 15;	
		
		// 计算负载
		private static final int NUM = 9; 
		
		public ConcurrentLinkedDeque<LatencySample> samples = new ConcurrentLinkedDeque<LatencySample>();

		public void addSample(LatencySample sample) {
			if ( samples.size() >= SAMPLE_SIZE) {
				samples.pollLast();
			}
			samples.offerFirst( sample );
		}
		
		public int calculateLatency() {
			
			 // 必须确认有足够的样本
	        if ( samples.size() >= NUM ) {
	        	
	        	int[] latencys = new int[ NUM ];
		       
		        int i = 0;
		        Iterator<LatencySample> itr = samples.iterator();
		        while( itr.hasNext() ) {
		        	if ( i == NUM )
		                break;
		            
		            latencys[i] = itr.next().latency;
		            i++;
		        }
		        
		        // 计算，去掉最高值&最低值, 利用中间值计算平均
		        int total = 0;
		        int max = latencys[0];
		        int min = latencys[0];
		        for(int j = 0; j < latencys.length; j++) {
		        	int v = latencys[j];
		        	if ( max < v ) max = v;
		        	if ( min > v ) min = v;
		        	total += v;
		        }
		        
		        // 纳秒
		        return ((total - min - max) / (NUM - 2));
	        }
	        
			return -1;
		}
		
		public List<LatencySample> getSparseSamples() {

			Map<Long, List<LatencySample>> sampleMap = new HashMap<Long, List<LatencySample>>();

			// 抽稀
			Iterator<LatencySample> itr = samples.iterator();
			while ( itr.hasNext() ) {
				LatencySample s = itr.next();
				long mill = s.time / 1000000;
				List<LatencySample> list = sampleMap.get(mill);
				if (list == null) {
					list = new ArrayList<LatencySample>();
					list.add(s);
					sampleMap.put(mill, list);
				} else {
					list.add(s);
				}
			}
			
			//
			List<LatencySample> sampleList = new ArrayList<LatencySample>();
			for (Map.Entry<Long, List<LatencySample>> entry : sampleMap.entrySet()) {

				int total = 0;
				int cnt = 0;
				for (LatencySample v : entry.getValue()) {
					total += v.latency;
					cnt++;
					
				}
				//
				LatencySample avgSample =  new LatencySample();
				avgSample.time = entry.getKey();
				avgSample.latency =  total / cnt ;
				sampleList.add( avgSample );
			}

			return sampleList;
		}
		
	}

	public static class LatencySample {
		public long time;
		public int latency;
	}
	
}