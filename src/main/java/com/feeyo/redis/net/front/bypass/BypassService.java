package com.feeyo.redis.net.front.bypass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
import com.feeyo.redis.nio.NameableExecutor;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.ExecutorUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisHolder;
import com.feeyo.util.jedis.JedisPool;

/*
 * 旁路服务
 */
public class BypassService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BypassService.class );
	
	public static final byte[] BUSY_RESP = "-ERR bypass busy.\r\n".getBytes();
	private static final byte[] TIMEOUT_RESP = "-ERR time out.\r\n".getBytes();
	
	private static BypassService _INSTANCE = null;
	
	private NameableExecutor threadPoolExecutor;
	
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
		this.corePoolSize = corePoolSizeString == null ? 2 : Integer.parseInt(corePoolSizeString);
		this.maxPoolSize = maxPoolSizeString == null ? 4 : Integer.parseInt(maxPoolSizeString);
		this.queueSize = queueSizeString == null ? 20 : Integer.parseInt( queueSizeString);
		this.timeout = timeoutString == null ? 3000 : Integer.parseInt(timeoutString);
		
		this.threadPoolExecutor = ExecutorUtil.create("bypass-Tp-", corePoolSize, maxPoolSize, 
				timeout, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>( queueSize ), true);
		
		StatUtil.getBigKeyCollector().setBigkeySize( requireSize );
	}
	
	public boolean testing(String requestCmd, byte[] requestKey, int requestSize) {
		
		if ( requestSize >= requireSize || StatUtil.getBigKeyCollector().isResponseBigkey(new String(requestKey), requestCmd)) {
			return true;
		}
		
		return false;
	}

	// 单例
	// 固定 coreSize maxSize queue threadPool
	// front to backend 同步请求
	// 支持  bigkey size 设置的能力、 reload 能力
	// 支持 request & response bigkey 检测 ， response bigkey, 需要 ttl scan 
	// 第一步支持 单请求， 后续考虑支持 pipeline 等 
	// 
	public void queuing(final RedisRequest request, final RedisFrontConnection frontConn, final PhysicalNode physicalNode) {
		
		try {
			
			threadPoolExecutor.execute( new Runnable() {
				@Override
				public void run() {
					if (timeout != -1
							&& TimeUtil.currentTimeMillis() - frontConn.getSession().getRequestTimeMills() > timeout) {
						frontConn.write(TIMEOUT_RESP);
						return;
					}

					JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(physicalNode.getHost(), physicalNode.getPort());
					JedisConnection conn = jedisPool.getResource();
					try {
						conn.sendCommand(request);
						List<RedisResponse> resps = conn.getResponses();
						if (resps != null) {
							String password = frontConn.getPassword();
							String cmd = frontConn.getSession().getRequestCmd();
							byte[] key = frontConn.getSession().getRequestKey();
							int requestSize = frontConn.getSession().getRequestSize();
							long requestTimeMills = frontConn.getSession().getRequestTimeMills();
							long responseTimeMills = TimeUtil.currentTimeMillis();
							int responseSize = 0;

							for (RedisResponse resp : resps)
								responseSize += writeToFront(frontConn, resp, 0);

							resps.clear(); // help GC
							resps = null;

							int procTimeMills = (int) (responseTimeMills - requestTimeMills);

							if (requestSize < requireSize && responseSize < requireSize) {
								StatUtil.getBigKeyCollector().delResponseBigkey(new String(key));
							}
							// 数据收集
							StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills,
									procTimeMills, false);
						}
					} catch (Exception e) {
						if (frontConn != null) {
							frontConn.close("write err");
						}
						// 由 reactor close
						LOGGER.error("backend write to front err:", e);
					} finally {
						if (conn != null) {
							conn.close();
						}
					}
				}
			});
			
		} catch (RejectedExecutionException rejectException) {	
			
			frontConn.write( BUSY_RESP );
			
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
			this.corePoolSize = corePoolSizeString == null ? 2 : Integer.parseInt(corePoolSizeString);
			this.maxPoolSize = maxPoolSizeString == null ? 4 : Integer.parseInt(maxPoolSizeString);
			this.queueSize = queueSizeString == null ? 20 : Integer.parseInt(queueSizeString);
			this.timeout = timeoutString == null ? 3000 : Integer.parseInt(timeoutString);

			if (corePoolSize != threadPoolExecutor.getCorePoolSize()) {
				NameableExecutor newThreadPoolExecutor = ExecutorUtil.create("bypass-Tp-", corePoolSize, maxPoolSize,
						timeout, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize), true);

				NameableExecutor oldThreadPoolExecutor = this.threadPoolExecutor;
				this.threadPoolExecutor = newThreadPoolExecutor;
				oldThreadPoolExecutor.shutdown();
			}
			StatUtil.getBigKeyCollector().setBigkeySize(requireSize);
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		}
		
		return "+OK\r\n".getBytes();
	}

	// 写入到前端
	private int writeToFront(RedisFrontConnection frontCon, RedisResponse response, int size) throws IOException {

		int tmpSize = size;

		if (frontCon.isClosed()) {
			throw new IOException("front conn is closed!");
		}

		if (response.type() == '+' || response.type() == '-' || response.type() == ':' || response.type() == '$') {

			byte[] buf = (byte[]) response.data();
			tmpSize += buf.length;

			frontCon.write(buf);

			// fast GC
			response.clear();

		} else {
			if (response.data() instanceof byte[]) {
				byte[] buf = (byte[]) response.data();
				tmpSize += buf.length;
				frontCon.write(buf);

				// fast GC
				response.clear();

			} else {
				RedisResponse[] items = (RedisResponse[]) response.data();
				for (int i = 0; i < items.length; i++) {
					if (i == 0) {
						byte[] buf = (byte[]) items[i].data();
						tmpSize += buf.length;
						frontCon.write(buf);

						// fast GC
						response.clear();

					} else {
						tmpSize = writeToFront(frontCon, items[i], tmpSize);
					}
				}
			}
		}
		return tmpSize;
	}

}
