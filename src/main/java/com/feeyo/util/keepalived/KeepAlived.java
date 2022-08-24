package com.feeyo.util.keepalived;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.JavaUtils;
import com.feeyo.util.ShellUtils;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * keepalived 检查
 * 
 * @author zhuam
 *
 */
public class KeepAlived {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KeepAlived.class );
	
	// 自检
	public static void check(final int port, final String authString) {
		
		if ( !JavaUtils.isLinux() ) {
			return;
		}
		
		Thread worker = new Thread() {
			
			private void sleepMs(long millis) {
				try {
					Thread.sleep( millis );
				} catch (InterruptedException e) {
				}
			}
			
			private boolean validate() {
				
				JedisConnection conn = null;
				try {				
					conn = new JedisConnection("127.0.0.1", port , 3000, 0);
					conn.sendCommand( RedisCommand.AUTH, authString);
					String reply = conn.getBulkReply();	
					if ( reply != null && reply.indexOf("OK") >= 0 ) {
						//
						conn.sendCommand( RedisCommand.SET, "selfcheck", String.valueOf( System.currentTimeMillis() ) );
						reply = conn.getBulkReply();
						if ( reply != null && reply.indexOf("OK") >= 0 ) {
							return true;
							
						} else {
							LOGGER.info("###self check### set err: {} ", new Object[]{ reply });	
						}
						
					} else {
						LOGGER.info("###self check### auth err: {} ", new Object[]{ reply });	
					}
					
				} catch (JedisConnectionException e) {
					LOGGER.info("###self check### err: {}, {}", port, authString, e);	
				} finally {
					if (conn != null) {
						conn.close();
					}
				}
				return false;
			}
			
			public void run() {
				
				sleepMs( 3 * 1000 );
			
				while ( true ) {
					
					if ( validate() ) {
						
						// keepalived check
						try {
							//keepalived (pid  4355) is running...

							String statusStr = ShellUtils.execCommand( "bash", "-c", "/etc/init.d/keepalived status");
							LOGGER.info("keepalived status, reply={}", new Object[]{ statusStr });
							
							if ( statusStr != null && statusStr.indexOf( "running") < 0 ) {
								String startStr = ShellUtils.execCommand( "bash", "-c", "/etc/init.d/keepalived start");
								LOGGER.info("keepalived status, reply={}", new Object[]{ startStr });
							}
							
						} catch (IOException e) {
							LOGGER.info("keepalived err:", e);
						}
						break;
						
					} else {
						sleepMs( 2 * 1000 );
					}
				}
			}
		};
		worker.start();
	}
	
}
