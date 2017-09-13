package com.feeyo.redis.net.front.prefix;

import java.util.HashMap;
import java.util.Map;

import com.feeyo.redis.net.front.prefix.impl.AllKey;
import com.feeyo.redis.net.front.prefix.impl.ExceptFirstKey;
import com.feeyo.redis.net.front.prefix.impl.ExceptLastKey;
import com.feeyo.redis.net.front.prefix.impl.MKey;
import com.feeyo.redis.net.front.prefix.impl.NoKey;
import com.feeyo.redis.net.front.prefix.impl.SecondKey;
import com.feeyo.redis.net.front.prefix.impl.FirstKey;
import com.feeyo.redis.net.front.prefix.impl.FristSecondKey;

public class KeyPrefixStrategyFactory {
	
	private static Map<String, Integer> keyStrategys = new HashMap<String, Integer>();
	
	// thread local
	// -------------------------------------------------------------------------
	private static ThreadLocal<AllKey> allKey = new ThreadLocal<AllKey>() {
		@Override
		protected AllKey initialValue() {
			return new AllKey();
		}
	};
	
	private static ThreadLocal<ExceptFirstKey> exceptFirstKey = new ThreadLocal<ExceptFirstKey>() {
		@Override
		protected ExceptFirstKey initialValue() {
			return new ExceptFirstKey();
		}
	};
	
	private static ThreadLocal<ExceptLastKey> exceptLastKey = new ThreadLocal<ExceptLastKey>(){
		@Override
		protected ExceptLastKey initialValue() {
			return new ExceptLastKey();
		}
	};
	
	private static ThreadLocal<FirstKey> firstKey = new ThreadLocal<FirstKey>() {
		@Override
		protected FirstKey initialValue() {
			return new FirstKey();
		}
	};
	
	private static ThreadLocal<FristSecondKey> fristSecondKey = new ThreadLocal<FristSecondKey>() {
		@Override
		protected FristSecondKey initialValue() {
			return new FristSecondKey();
		}
	};
	
	private static ThreadLocal<MKey> mKey = new ThreadLocal<MKey>() {
		@Override
		protected MKey initialValue() {
			return new MKey();
		}
	};
	
	private static ThreadLocal<NoKey> noKey = new ThreadLocal<NoKey>() {
		@Override
		protected NoKey initialValue() {
			return new NoKey();
		}
	};
	
	private static ThreadLocal<SecondKey> secondKey = new ThreadLocal<SecondKey>() {
		@Override
		protected SecondKey initialValue() {
			return new SecondKey();
		}
	};
	
	static {
		keyStrategys.put("CLUSTER", KeyPrefixStrategy.NoKey);
		keyStrategys.put("INFO", KeyPrefixStrategy.NoKey);
		keyStrategys.put("TIME", KeyPrefixStrategy.NoKey);
		keyStrategys.put("CLIENT", KeyPrefixStrategy.NoKey);
		keyStrategys.put("CONFIG", KeyPrefixStrategy.NoKey);
		keyStrategys.put("DEBUG", KeyPrefixStrategy.NoKey);
		keyStrategys.put("SCRIPT", KeyPrefixStrategy.NoKey);
		
		// 消息订阅
		keyStrategys.put("SUBSCRIBE", KeyPrefixStrategy.NoKey);		//old AllKey
		keyStrategys.put("PSUBSCRIBE", KeyPrefixStrategy.NoKey);
		keyStrategys.put("PUBLISH", KeyPrefixStrategy.NoKey);
		keyStrategys.put("UNSUBSCRIBE", KeyPrefixStrategy.NoKey);
		keyStrategys.put("PUNSUBSCRIBE", KeyPrefixStrategy.NoKey);
		
		// 事务
		keyStrategys.put("MULTI", KeyPrefixStrategy.NoKey);
		keyStrategys.put("EXEC", KeyPrefixStrategy.NoKey);
		keyStrategys.put("DISCARD", KeyPrefixStrategy.NoKey);
		keyStrategys.put("WATCH", KeyPrefixStrategy.AllKey);
		//keyStrategys.put("UNWATCH", KeyPrefixStrategy.AllKey);
		
		
		keyStrategys.put("BRPOP", KeyPrefixStrategy.ExceptLastKey);
		keyStrategys.put("BLPOP", KeyPrefixStrategy.ExceptLastKey);
		keyStrategys.put("BITOP", KeyPrefixStrategy.ExceptFirstKey);
		
		keyStrategys.put("SMOVE", KeyPrefixStrategy.FristSecondKey);
		keyStrategys.put("RPOPLPUSH", KeyPrefixStrategy.FristSecondKey);
		keyStrategys.put("BRPOPLPUSH", KeyPrefixStrategy.FristSecondKey);
		
		keyStrategys.put("EXISTS", KeyPrefixStrategy.AllKey);
		keyStrategys.put("PFMERGE", KeyPrefixStrategy.AllKey);
		keyStrategys.put("PFCOUNT", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SDIFFSTORE", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SDIFF", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SINTERSTORE", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SINTER", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SUNIONSTORE", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SUNION", KeyPrefixStrategy.AllKey);
		keyStrategys.put("SCAN", KeyPrefixStrategy.NoKey);
		
		keyStrategys.put("DEL", KeyPrefixStrategy.AllKey);
		keyStrategys.put("RENAMENX", KeyPrefixStrategy.AllKey);
		keyStrategys.put("RENAME", KeyPrefixStrategy.AllKey);
		
		keyStrategys.put("MGET", KeyPrefixStrategy.AllKey);		
		keyStrategys.put("MSET", KeyPrefixStrategy.MKey);
		keyStrategys.put("MSETNX", KeyPrefixStrategy.MKey);		
		keyStrategys.put("OBJECT", KeyPrefixStrategy.SecondKey);
	}
	
	// 
	// TODO: 修复一个并发问题，不采用锁，直接 new, 耗内存哦，采用ThreadLocal
	public static KeyPrefixStrategy getStrategy(String cmd) {		
		
		Integer strategyCode = keyStrategys.get( cmd.toUpperCase() );
		if ( strategyCode == null ) {
			return firstKey.get();
		}
		
		switch( strategyCode ) {
		case KeyPrefixStrategy.AllKey:
			return allKey.get();
		case KeyPrefixStrategy.ExceptFirstKey:
			return exceptFirstKey.get();
		case KeyPrefixStrategy.ExceptLastKey:
			return exceptLastKey.get();
		case KeyPrefixStrategy.FirstKey:
			return firstKey.get();
		case KeyPrefixStrategy.FristSecondKey:
			return fristSecondKey.get();
		case KeyPrefixStrategy.MKey:
			return mKey.get();
		case KeyPrefixStrategy.NoKey:
			return noKey.get();
		case KeyPrefixStrategy.SecondKey:
			return secondKey.get();
		default:
			return firstKey.get();
		}
	}

}
