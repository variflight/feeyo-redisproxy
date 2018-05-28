package com.feeyo.redis;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.Log4jInitializer;

/**
 * redis-benchmark -p 8066 -c 100 -t set,get,lpush,LPOP,sAdd,spop,incr -n 500000
 * redis-benchmark -p 8066 -c 100 -t set,get,lpush,LPOP,sAdd,spop,incr -n 500000 -a pwd01
 * redis-benchmark -p 8066 -c 100 -t set,get,lpush,LPOP,sAdd,spop,incr -n 500000 -a pwd01 --dbnum 1
 * redis-benchmark -p 8066 -c 100 -n 500000 -a pwd01
 * 
 * redis-benchmark -p 6379 -c 100 -t set,get,lpush,LPOP,sAdd,spop,incr -n 500000
 * 
 * @author zhuam 
 *
 */

public class RedisServer {
	
	//定时线程池，单线程线程池
	private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	
	//心跳独立，避免被其他任务影响
	private static final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
	
	public static void main(String[] args) throws IOException {
		
		//System.setProperty("com.sun.management.jmxremote.port", "8099");
		//System.setProperty("com.sun.management.jmxremote.ssl", "false");
		//System.setProperty("com.sun.management.jmxremote.authenticate", "false");
		
		// 检查 FEEYO_HOME
        if ( System.getProperty("FEEYO_HOME") == null ) {
            System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));  
        }
        
        // 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		try {
			
			// 引擎初始化
			RedisEngineCtx.INSTANCE().init();
			
			
			// 弱精度的计时器
			heartbeatScheduler.scheduleAtFixedRate(new Runnable(){
				@Override
				public void run() {		
					TimeUtil.update();
				}			
			}, 0, 10L, TimeUnit.MILLISECONDS);
			
			
			// IDLE 连接检查, 关闭
			scheduler.scheduleAtFixedRate(new Runnable(){
				@Override
				public void run() {		
					NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
						@Override
						public void run() {
							NetSystem.getInstance().checkConnections();
						}
					});
				}			
			}, 0L, 1 * 1000L, TimeUnit.MILLISECONDS);	
			
			// 连接池有效性
			heartbeatScheduler.scheduleAtFixedRate(new Runnable(){
				@Override
				public void run() {		
					
					NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
						@Override
						public void run() {
							
							Map<Integer, AbstractPool> pools = RedisEngineCtx.INSTANCE().getPoolMap();
							for(AbstractPool pool : pools.values() ) {
								pool.availableCheck();
							}

						}
					});
				}			
			}, 10L, 10L, TimeUnit.SECONDS);	
			
			
			/*
			 * 连接池心跳 检测
			 * 1、IDLE 连接的有效性检测，无效 close
			 * 2、连接池过大、过小的动态调整
			 */
			heartbeatScheduler.scheduleAtFixedRate(new Runnable(){
				static final long TIMEOUT = 2 * 60 * 1000L;
				
				@Override
				public void run() {
					
					NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
						@Override
						public void run() {
							Map<Integer, AbstractPool> pools = RedisEngineCtx.INSTANCE().getPoolMap();
							for(AbstractPool pool : pools.values() ) {
								pool.heartbeatCheck( TIMEOUT );
							}
						}
					});
				}			
			}, 30L, 30L, TimeUnit.SECONDS);
			
			// CONSOLE 
			System.out.println("Home directory=" + System.getProperty("FEEYO_HOME") + ", startup=" + System.currentTimeMillis());
			
			
		} catch (Throwable e) {
			
			// exit
			System.exit( 0 );
		}
		
		
	
	}
}
