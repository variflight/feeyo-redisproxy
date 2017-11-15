package com.feeyo.redis.engine.manage.stat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.feeyo.redis.engine.manage.stat.KeyUnit.KeyType;

public class TopHundredProcessUtil {
	
	//取field value命令
	private static final String HGETALL_COMMAND = "*2\r\n$7\r\nhgetall\r\n";
	private final static String LRANGE_COMMAND = "*4\r\n$6\r\nlrange\r\n"; 
	private final static String SMEMBERS_COMMAND = "*2\r\n$8\r\nsmembers\r\n";
	private final static String ZRANGE_COMMAND  = "*5\r\n$6\r\nzrange\r\n"; 
	
	private static final Map<String, KeyType> _cmds = new HashMap<String, KeyType>();
	private static final List<String> _add_cmds = new ArrayList<String>();
	
	static {
		_add_cmds.add("HSET");
		_add_cmds.add("LPUSH");
		_add_cmds.add("SADD");
		_add_cmds.add("ZADD");
	}
	
	static {
		//Hash
		_cmds.put("HDEL", 				KeyType.HASH);
		_cmds.put("HEXISTS", 			KeyType.HASH);
		_cmds.put("HGET", 				KeyType.HASH);
		_cmds.put("HGETALL", 			KeyType.HASH);
		_cmds.put("HINCRBY", 			KeyType.HASH);
		_cmds.put("HINCRBYFLOAT", 		KeyType.HASH);
		_cmds.put("HKEYS", 				KeyType.HASH);
		_cmds.put("HLEN", 				KeyType.HASH);
		_cmds.put("HMGET",				KeyType.HASH);
		_cmds.put("HMSET", 				KeyType.HASH);
		_cmds.put("HSET", 				KeyType.HASH);
		_cmds.put("HSETNX", 			KeyType.HASH);
		_cmds.put("HVALS", 				KeyType.HASH);
		_cmds.put("HSCAN", 				KeyType.HASH);
		_cmds.put("HSTRLEN", 			KeyType.HASH);
		
		// List
		_cmds.put("BLPOP", 				KeyType.LIST);
		_cmds.put("BRPOP", 				KeyType.LIST);
		_cmds.put("BRPOPLPUSH", 		KeyType.LIST);
		_cmds.put("LINDEX", 			KeyType.LIST);
		_cmds.put("LINSERT", 			KeyType.LIST);
		_cmds.put("LLEN", 				KeyType.LIST);
		_cmds.put("LPOP", 				KeyType.LIST);
		_cmds.put("LPUSH", 				KeyType.LIST);
		_cmds.put("LPUSHX", 			KeyType.LIST);
		_cmds.put("LRANGE", 			KeyType.LIST);
		_cmds.put("LREM", 				KeyType.LIST);
		_cmds.put("LSET", 				KeyType.LIST);
		_cmds.put("LTRIM", 				KeyType.LIST);
		_cmds.put("RPOP", 				KeyType.LIST);
		_cmds.put("RPOPLPUSH", 			KeyType.LIST);
		_cmds.put("RPUSH", 				KeyType.LIST);
		_cmds.put("RPUSHX", 			KeyType.LIST);
		
		// Set
		_cmds.put("SADD", 				KeyType.SET);
		_cmds.put("SCARD", 				KeyType.SET);
		_cmds.put("SISMEMBER", 			KeyType.SET);
		_cmds.put("SMEMBERS", 			KeyType.SET);
		_cmds.put("SMOVE", 				KeyType.SET);
		_cmds.put("SPOP", 				KeyType.SET);
		_cmds.put("SRANDMEMBER", 		KeyType.SET);
		_cmds.put("SREM", 				KeyType.SET);
		_cmds.put("SSCAN", 				KeyType.SET);
		_cmds.put("SDIFF", 				KeyType.SET);
		_cmds.put("SDIFFSTORE", 		KeyType.SET);
		_cmds.put("SINTER", 			KeyType.SET);
		_cmds.put("SINTERSTORE", 		KeyType.SET);
		_cmds.put("SUNION", 			KeyType.SET);
		_cmds.put("SUNIONSTORE", 		KeyType.SET);
		
		// SortedSet
		_cmds.put("ZADD", 				KeyType.ZSET);
		_cmds.put("ZCARD", 				KeyType.ZSET);
		_cmds.put("ZCOUNT", 			KeyType.ZSET);
		_cmds.put("ZINCRBY", 			KeyType.ZSET);
		_cmds.put("ZRANGE", 			KeyType.ZSET);
		_cmds.put("ZRANGEBYSCORE",  	KeyType.ZSET);
		_cmds.put("ZRANK", 				KeyType.ZSET);
		_cmds.put("ZREM", 				KeyType.ZSET);
		_cmds.put("ZREMRANGEBYRANK",	KeyType.ZSET);
		_cmds.put("ZREMRANGEBYSCORE", 	KeyType.ZSET);
		_cmds.put("ZREVRANGE", 			KeyType.ZSET);
		_cmds.put("ZREVRANGEBYSCORE", 	KeyType.ZSET);
		_cmds.put("ZREVRANK", 			KeyType.ZSET);
		_cmds.put("ZSCORE", 			KeyType.ZSET);
		_cmds.put("ZUNIONSTORE", 		KeyType.ZSET);
		_cmds.put("ZINTERSTORE", 		KeyType.ZSET);
		_cmds.put("ZSCAN", 				KeyType.ZSET);
		_cmds.put("ZRANGEBYLEX", 		KeyType.ZSET);
		_cmds.put("ZLEXCOUNT", 			KeyType.ZSET);
		_cmds.put("ZREMRANGEBYLEX", 	KeyType.ZSET);
	}
	
	public static KeyType transformCommandToType(String cmd) {
		KeyType type =  _cmds.get(cmd);
		return type == null ? KeyType.UNHANDLE : type;
	}
	
	public static boolean isAddCommand(String cmd) {
		return _add_cmds.contains(cmd);
	}
	
	public static ByteBuffer getRequestByteBuffer(String key,KeyType type) {
		byte[] buffer = getRequestCommand(key, type);
		ByteBuffer typeBuffer = ByteBuffer.allocate(buffer.length) ;
		return writeToBuffer(buffer, typeBuffer);
	}
	
	private static byte[] getRequestCommand(String key, KeyType type) {
		switch(type) {
		case HASH:
			return getHGetallCommand(key);
		case LIST:
			return getLRangeCommand(key);
		case SET:
			return getSMembersCommand(key);
		case ZSET:
			return getZRangeCommand(key);
		default:
			return null;
		}
	}
	
	private static ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;			
		while (length > 0) {
			buffer.put(src, offset, length);
			break;
		} 
		return buffer;
	}
	
	private static byte[] getHGetallCommand(String key) {
		return (HGETALL_COMMAND+'$'+key.length()+"\r\n"+key+"\r\n").getBytes();
	}
	
	private static byte[] getLRangeCommand(String key) {
		return (LRANGE_COMMAND+'$'+key.length()+"\r\n"+key+"\r\n"+"$1\r\n0\r\n$2\r\n-1\r\n").getBytes();
	}
	
	private static byte[] getSMembersCommand(String key) {
		return (SMEMBERS_COMMAND+'$'+key.length()+"\r\n"+key+"\r\n").getBytes();
	}
	
	private static byte[] getZRangeCommand(String key) {
		return (ZRANGE_COMMAND+'$'+key.length()+"\r\n"+key+"\r\n"+"$1\r\n0\r\n$2\r\n-1\r\n"+'$'+"WITHSCORES".length()+"\r\n"+"WITHSCORES"+"\r\n").getBytes();
	}
	
	public static KeyUnit handleReceivedString(String key, KeyType type, String receivedString) {
		long length = 0L;
		long count_1k = 0L;
		long count_10k = 0L;
	    String regex = "\\$[1-9][0-9]*\r\n";  
        Pattern p = Pattern.compile(regex);   
        Matcher matcher = p.matcher(receivedString);
        while (matcher.find()) {  
        	length ++;
            String msg = matcher.group();
            msg = msg.substring(1,msg.length()-2);
            long fieldLength = Integer.parseInt(msg);
            if(fieldLength > 1024 * 10)
            	count_10k++;
            else if(fieldLength > 1024)
            	count_1k++;
        }  
		return new KeyUnit(null, key, type, length, count_1k, count_10k, null);
	}
}
