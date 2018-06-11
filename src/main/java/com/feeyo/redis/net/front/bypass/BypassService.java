package com.feeyo.redis.net.front.bypass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisResponse;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.ExecutorUtil;

/*
 * 旁路服务
 */
public class BypassService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BypassService.class );
	
	public static final byte[] ERR_RESP = "-ERR bypass busy.\r\n".getBytes();
	
	private static BypassService _INSTANCE = null;
	
	//
	private ThreadPoolExecutor threadPoolExecutor;
	
	private int requireSize;
	private int corePoolSize;
	private int maxPoolSize;
	private int queueSize;
	
	private int timeout;		// 单位秒
	
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
		String requireSizeString = map.get("bypassRequireSize"); 
		String corePoolSizeString = map.get("bypassCorePoolSize"); 
		String maxPoolSizeString = map.get("bypassMaxPoolSize");
		String queueSizeString = map.get("bypassQueueSize"); 
		String timeoutString = map.get("bypassTimeoutSize"); 
		
		this.requireSize = requireSizeString == null ? 256 * 1024 : Integer.parseInt(requireSizeString);
		if ( requireSize < 256 * 1024)
			requireSize = 256 * 1024;
		
		this.corePoolSize = corePoolSizeString == null ? 2 : Integer.parseInt(corePoolSizeString);
		this.maxPoolSize = maxPoolSizeString == null ? 4 : Integer.parseInt(maxPoolSizeString);
		this.queueSize = queueSizeString == null ? 20 : Integer.parseInt( queueSizeString);
		this.timeout = timeoutString == null ? 3000 : Integer.parseInt(timeoutString);
		
		this.threadPoolExecutor = ExecutorUtil.create("bypass-Tp-", corePoolSize, maxPoolSize, 
				timeout, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>( queueSize ), true);
		
		StatUtil.getBigKeyCollector().setSize( requireSize );
	}
	
	// 检测
	public boolean testing(String requestCmd, String requestKey, int requestSize) {
		
		if ( requestSize >= requireSize || StatUtil.getBigKeyCollector().isResponseBigkey(requestCmd, requestKey)) {
			return true;
		}
		
		return false;
	}

	// 排队执行
	public void queuing(final RedisRequest request, final RedisFrontConnection frontConn, final PhysicalNode physicalNode) {
		
		try {
			
			threadPoolExecutor.execute(new Runnable() {
				@Override
				public void run() {

					BypassIoConnection conn = new BypassIoConnection(physicalNode.getHost(), physicalNode.getPort());
					List<RedisResponse> resps = conn.writeToBackend(request);

					if (resps != null) {
						try {
							String password = frontConn.getPassword();
							String cmd = frontConn.getSession().getRequestCmd();
							String key = frontConn.getSession().getRequestKey();
							int requestSize = frontConn.getSession().getRequestSize();
							long requestTimeMills = frontConn.getSession().getRequestTimeMills();
							long responseTimeMills = TimeUtil.currentTimeMillis();
							int responseSize = 0;
							
							for (RedisResponse resp : resps)
								responseSize += conn.writeToFront(frontConn, resp, 0);
							
							resps.clear(); // help GC
							resps = null;
							
							//
							if (requestSize < requireSize && responseSize < requireSize) {
								StatUtil.getBigKeyCollector().deleteResponseBigkey( key );
							}
							
							// 数据收集
							int procTimeMills = (int) (responseTimeMills - requestTimeMills);
							StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills, procTimeMills, false);
							
						} catch(IOException e) {
							
							if ( frontConn != null) {
								frontConn.close("write err");
							}

							// 由 reactor close
							LOGGER.error("backend write to front err:", e);
						}
					}
				}
			});
			
		} catch (RejectedExecutionException rejectException) {	
			
			frontConn.write( ERR_RESP );
			
			LOGGER.warn("process thread pool is full, reject, active={} poolSize={} corePoolSize={} maxPoolSize={} taskCount={}",
					new Object[]{ threadPoolExecutor.getActiveCount(), threadPoolExecutor.getPoolSize(), threadPoolExecutor.getCorePoolSize(), 
							threadPoolExecutor.getMaximumPoolSize(),threadPoolExecutor.getTaskCount()} );						
		}	
		
		
	}
	
	public byte[] reload() {
		
		try {
			Map<String, String> map = ConfigLoader.loadServerMap(ConfigLoader.buidCfgAbsPathFor("server.xml"));
			String requireSizeString = map.get("bypassRequireSize");
			String corePoolSizeString = map.get("bypassCorePoolSize");
			String maxPoolSizeString = map.get("bypassMaxPoolSize");
			String queueSizeString = map.get("bypassQueueSize");
			String timeoutString = map.get("bypassTimeoutSize");

			this.requireSize = requireSizeString == null ? 256 * 1024 : Integer.parseInt(requireSizeString);
			if ( requireSize < 256 * 1024 )
				requireSize = 256 * 1024;
			
			this.corePoolSize = corePoolSizeString == null ? 2 : Integer.parseInt(corePoolSizeString);
			this.maxPoolSize = maxPoolSizeString == null ? 4 : Integer.parseInt(maxPoolSizeString);
			this.queueSize = queueSizeString == null ? 20 : Integer.parseInt(queueSizeString);
			this.timeout = timeoutString == null ? 3000 : Integer.parseInt(timeoutString);

			if (corePoolSize != threadPoolExecutor.getCorePoolSize()) {
				
				// hold old
				ThreadPoolExecutor oldThreadPoolExecutor = this.threadPoolExecutor;
				
				// create new
				ThreadPoolExecutor newThreadPoolExecutor = ExecutorUtil.create("bypass-Tp-", corePoolSize, maxPoolSize,
						timeout, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize), true);
				this.threadPoolExecutor = newThreadPoolExecutor;
				
				// kill old
				oldThreadPoolExecutor.shutdown();
			}
			
			StatUtil.getBigKeyCollector().setSize( requireSize );
			
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		}
		
		return "+OK\r\n".getBytes();
	}

}
