package com.feeyo.redis.net.front.bypass;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisResponse;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.loader.ConfigLoader;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/*
 * 旁路服务
 */
public class BypassService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BypassService.class );

	private static BypassService _INSTANCE = null;
	
	//
	private volatile BypassThreadExecutor threadPoolExecutor;
	
	private int requireSize;
	private int corePoolSize;
	private int maxPoolSize;
	private int queueSize;
	
	public static BypassService INSTANCE() {
		
		if ( _INSTANCE == null ) {
			synchronized ( BypassService.class) {
				if ( _INSTANCE == null ) 
					_INSTANCE = new BypassService();
			}
		}
		
		return _INSTANCE;
	}
	
	private BypassService () {
	
		Map<String, String> map = RedisEngineCtx.INSTANCE().getServerMap();
		updateParameter( map );
		
		//
		this.threadPoolExecutor = new BypassThreadExecutor(
				corePoolSize, maxPoolSize, queueSize, new ThreadFactoryImpl("BypassService"));
		this.threadPoolExecutor.prestartAllCoreThreads();
		
		StatUtil.getBigKeyCollector().setSize( requireSize );
	}

	
	// 检测
	public boolean testing(String requestCmd, String requestKey, int requestSize) {
		
		if ( requestSize >= requireSize || StatUtil.getBigKeyCollector().isResponseBigkey(requestCmd, requestKey)) {
			return true;
		}
		
		return false;
	}

	// 进入排队
	public void queueUp(final RedisRequest request, 
			final RedisFrontConnection frontConn, final String host, final int port) {
		
		try {
			
			threadPoolExecutor.execute(new Runnable() {
				
				@Override
				public void run() {
					//判断前端连接
                    if (frontConn == null) {
                        return;
                    }
                    String password = frontConn.getPassword();
                    String cmd = frontConn.getSession().getRequestCmd();
                    String key = frontConn.getSession().getRequestKey();
                    int requestSize = frontConn.getSession().getRequestSize();
                    long requestTimeMills = frontConn.getSession().getRequestTimeMills();
                    int responseSize = 0;

					try {
						
						BypassIoConnection backConn = new BypassIoConnection(host, port);
						List<RedisResponse> resps = backConn.writeToBackend(request);

						if (resps != null) {

							long responseTimeMills = TimeUtil.currentTimeMillis();

							for (RedisResponse resp : resps)
								responseSize += backConn.writeToFront(frontConn, resp, 0);
							
							resps.clear(); // help GC
							resps = null;
							
							//
							if (requestSize < requireSize && responseSize < requireSize) {
								StatUtil.getBigKeyCollector().deleteResponseBigkey( key );
							}
							
							// 数据收集
							int procTimeMills = (int) (responseTimeMills - requestTimeMills);
							StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills, procTimeMills, false, true,false);
						}
						
					} catch(IOException e) {

                        long responseTimeMills = TimeUtil.currentTimeMillis();
                        int procTimeMills = (int) (responseTimeMills - requestTimeMills);
                        if (frontConn != null) {
                            frontConn.close("write err");
                        }

                        StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills, procTimeMills, false, true,true);

                        LOGGER.error("bypass write to front err:", e);
					}
				}
			});
			
		} catch (RejectedExecutionException re) {	
			
			// front rejected 
			frontConn.write( "-ERR Bypass traffic congestion, rejected execution. \r\n".getBytes() );
			
			LOGGER.error("Bypass rejected, active={} poolSize={} corePoolSize={} maxSubmittedTaskCount={} submittedTasksCount={}, completedTaskCount={}, frontConn={}/{}/{}/{}",
					new Object[]{ threadPoolExecutor.getActiveCount(), threadPoolExecutor.getPoolSize(), threadPoolExecutor.getCorePoolSize(),
							threadPoolExecutor.getMaxSubmittedTaskCount(),threadPoolExecutor.getSubmittedTasksCount(), threadPoolExecutor.getCompletedTaskCount(),
							frontConn.getHost(), frontConn.getPassword(), frontConn.getSession().getRequestCmd(), frontConn.getSession().getRequestKey() } );
		}	
	}
	
	public byte[] reload() {
		
		try {
			
			Map<String, String> map = ConfigLoader.loadServerMap(ConfigLoader.buidCfgAbsPathFor("server.xml"));
			boolean isUpdated = updateParameter( map );
			if ( isUpdated ) {
				
				// hold old threadPool
				ThreadPoolExecutor oldThreadPoolExecutor = this.threadPoolExecutor;
				
				// create new threadPool
				BypassThreadExecutor newThreadPoolExecutor = new BypassThreadExecutor(
						corePoolSize, maxPoolSize, queueSize, new ThreadFactoryImpl("BypassService"));
				newThreadPoolExecutor.prestartAllCoreThreads();
								
				// swap threadPool
				this.threadPoolExecutor = newThreadPoolExecutor;
				
				// kill old threadPool
				oldThreadPoolExecutor.shutdown();
				
				StatUtil.getBigKeyCollector().setSize( requireSize );
				
				return "+OK\r\n".getBytes();
				
			} else  {	
				return "+ERR parameter err, pls check. \r\n".getBytes();
			}
			
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		}

	}
	
	// set
	private boolean updateParameter(Map<String, String> map) {
		
		String requireSizeString = map.get("bypassRequireSize");
		String corePoolSizeString = map.get("bypassCorePoolSize");
		String maxPoolSizeString = map.get("bypassMaxPoolSize");
		String queueSizeString = map.get("bypassQueueSize");

		int new_requireSize = requireSizeString == null ? 256 * 1024 : Integer.parseInt(requireSizeString);
		int new_corePoolSize = corePoolSizeString == null ? 2 : Integer.parseInt(corePoolSizeString);
		int new_maxPoolSize = maxPoolSizeString == null ? 4 : Integer.parseInt(maxPoolSizeString);
		int new_queueSize = queueSizeString == null ? 20 : Integer.parseInt(queueSizeString);
		
		// code safe
		if ( new_requireSize < 100 * 1024) new_requireSize = 100 * 1024;
		if ( new_corePoolSize > 4 ) new_corePoolSize = 4;
		if ( new_maxPoolSize > 6 ) new_maxPoolSize = 6;
		if ( new_queueSize > 100 ) new_queueSize = 100;

        // output
        System.out.println( String.format("bypassRequireSize=%s, bypassCorePoolSize=%s, bypassMaxPoolSize=%s, bypassQueueSize=%s",
                new_requireSize, new_corePoolSize, new_maxPoolSize, new_queueSize) );

		if ( this.requireSize == new_requireSize &&
			 this.corePoolSize == new_corePoolSize &&
			 this.maxPoolSize == new_maxPoolSize &&
			 this.queueSize == new_queueSize ) {
			return false;
			
		} else {
			this.requireSize = new_requireSize;
			this.corePoolSize = new_corePoolSize;
			this.maxPoolSize = new_maxPoolSize;
			this.queueSize = new_queueSize;		
			return true;
		}
	}

}