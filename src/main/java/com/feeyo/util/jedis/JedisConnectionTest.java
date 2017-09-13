package com.feeyo.util.jedis;

public class JedisConnectionTest {
	
	public static void main(String args[]) {		
		
		//127.0.0.1
		String host = "127.0.0.1";
		int port = 6379;
		JedisConnection conn = null;		
		try {
			
			// 
			conn = new JedisConnection(host, port);
			conn.sendCommand( RedisCommand.GET, "aa");
			String value = conn.getBulkReply();
			System.out.println( value );
			
			// 
			conn.sendCommand( RedisCommand.SET, "aa2", "bbb2测试");
			String value2 = conn.getBulkReply();
			System.out.println( value2 );
			
			//
			conn.sendCommand( RedisCommand.GET, "aa2");
			String value3 = conn.getBulkReply();
			System.out.println( value3 );
			
			// ERR unknown command 'CLUSTER'
			conn.sendCommand( RedisCommand.CLUSTER, "info");
			String value4 = conn.getStatusCodeReply();
			System.out.println( value4 );
			
			// 
			conn.sendCommand( RedisCommand.CLUSTER, "nodes");
			String value5 = conn.getBulkReply();
			System.out.println( value5 );
			
		} finally {
			if ( conn != null ) {
				conn.disconnect();
			}
		}
	}

}
