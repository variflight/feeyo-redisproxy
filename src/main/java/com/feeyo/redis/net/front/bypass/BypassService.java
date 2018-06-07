package com.feeyo.redis.net.front.bypass;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.BigKeyCollector;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.nio.NameableExecutor;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.ExecutorUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisHolder;
import com.feeyo.util.jedis.JedisPool;

// 旁路服务
public class BypassService {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BypassService.class );
	
	final private static BypassService instance = new BypassService();
	
	private NameableExecutor bigkeyExecutor;
	private int timeout;
	private BigKeyCollector bigKeyCollector;
	private int bigkeySize;
	private int bigkeyQueueSize;
	private int bigkeyThreadSize;
	
	public static BypassService INSTANCE() {
		return instance;
	}
	
	private BypassService () {
		Map<String, String> serverMap = RedisEngineCtx.INSTANCE().getServerMap();
		String bigkeySizeString = serverMap.get("bigkeySize"); 
		String bigkeyThreadSizeString = serverMap.get("bigkeyThreadSize"); 
		String bigkeyQueueSizeString = serverMap.get("bigkeyQueueSize"); 
		String bigkeyQueueTimeoutString = serverMap.get("bigkeyQueueTimeout"); 
		this.bigkeySize = bigkeySizeString == null ? 256 * 1024 : Integer.parseInt(bigkeySizeString);
		this.bigkeyThreadSize = bigkeyThreadSizeString == null ? 4 : Integer.parseInt(bigkeyThreadSizeString);
		this.bigkeyQueueSize = bigkeyQueueSizeString == null ? 100 : Integer.parseInt(bigkeyQueueSizeString);
		this.timeout = bigkeyQueueTimeoutString == null ? 3000 : Integer.parseInt(bigkeyQueueTimeoutString);
		
		this.bigkeyExecutor = ExecutorUtil.create("BigkeyExecutor-", bigkeyThreadSize);
		this.bigKeyCollector = StatUtil.getBigKeyCollector();
		bigKeyCollector.setBigkeySize(bigkeySize);
	}
	
	

	// 单例
	// 固定 coreSize maxSize queue threadPool
	// front to backend 同步请求
	// 支持  bigkey size 设置的能力、 reload 能力
	// 支持 request & response bigkey 检测 ， response bigkey, 需要 ttl scan 
	// 第一步支持 单请求， 后续考虑支持 pipeline 等 
	// 
	public boolean goQueuing(final RedisRequest request, final RedisFrontConnection frontConn, final PhysicalNode physicalNode) throws BeyondTaskQueueException {
		if (frontConn.getSession().getRequestSize() >= bigkeySize 
				|| bigKeyCollector.isResponseBigkey(new String(frontConn.getSession().getRequestKey()), frontConn.getSession().getRequestCmd())) {
			
			if (bigkeyExecutor.getQueue().size() >= bigkeyQueueSize) {
				throw new BeyondTaskQueueException();
			}
			
			bigkeyExecutor.execute( new Runnable() {
				@Override
				public void run() {
					if (timeout != -1 && TimeUtil.currentTimeMillis() - frontConn.getSession().getRequestTimeMills() > timeout) {
						frontConn.write("-ERR time out.\r\n".getBytes());
						return;
					}
					JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(physicalNode.getHost(), physicalNode.getPort());
					JedisConnection conn = jedisPool.getResource();
					try {
						
					} catch (Exception e) {
						LOGGER.error("return remote offset err:", e);
					} finally {
						if (conn != null) {
							conn.close();
						}
					}
					
				}
			});
			return true;
		} else {
			return false;
		}
	}
	
	public byte[] reload() {
		try {
			Map<String, String> serverMap = ConfigLoader.loadServerMap(ConfigLoader.buidCfgAbsPathFor("server.xml"));
			String bigkeySizeString = serverMap.get("bigkeySize"); 
			String bigkeyThreadSizeString = serverMap.get("bigkeyThreadSize"); 
			String bigkeyQueueSizeString = serverMap.get("bigkeyQueueSize"); 
			String bigkeyQueueTimeoutString = serverMap.get("bigkeyQueueTimeout"); 
			this.bigkeySize = bigkeySizeString == null ? 256 * 1024 : Integer.parseInt(bigkeySizeString);
			this.bigkeyThreadSize = bigkeyThreadSizeString == null ? 4 : Integer.parseInt(bigkeyThreadSizeString);
			this.bigkeyQueueSize = bigkeyQueueSizeString == null ? 100 : Integer.parseInt(bigkeyQueueSizeString);
			this.timeout = bigkeyQueueTimeoutString == null ? 3000 : Integer.parseInt(bigkeyQueueTimeoutString);
			
			if (bigkeyThreadSize != bigkeyExecutor.getCorePoolSize()) {
				NameableExecutor newBigkeyExecutor = ExecutorUtil.create("BigkeyExecutor-", bigkeyThreadSize);
				NameableExecutor oldBigkeyExecutor = this.bigkeyExecutor;
				this.bigkeyExecutor = newBigkeyExecutor;
				oldBigkeyExecutor.shutdown();
			}
			bigKeyCollector.setBigkeySize(bigkeySize);
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		}
		return "+OK\r\n".getBytes();
	}
	
	public static void main(String[] args) throws InterruptedException {
			NameableExecutor ne = ExecutorUtil.create("BigkeyExecutor-", 2);
			for (int i = 0 ; i< 10;i++) {
				try {
					ne.execute( new Runnable() {
						@Override
						public void run() {
							try {
								System.out.println(1111);
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					});
					System.out.println(ne.getQueue().size());
				} catch (RejectedExecutionException r) {
					System.out.println("error");
					Thread.sleep(1000);
				}
			}
			while (ne.getQueue().size() != 0) {
				Thread.sleep(1000);
			}
	}
	
 	
}
