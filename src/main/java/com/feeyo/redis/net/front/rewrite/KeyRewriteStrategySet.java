package com.feeyo.redis.net.front.rewrite;

import java.util.HashMap;
import java.util.Map;

import com.feeyo.redis.net.front.rewrite.impl.AllKey;
import com.feeyo.redis.net.front.rewrite.impl.EvalKey;
import com.feeyo.redis.net.front.rewrite.impl.ExceptFirstKey;
import com.feeyo.redis.net.front.rewrite.impl.ExceptLastKey;
import com.feeyo.redis.net.front.rewrite.impl.FirstKey;
import com.feeyo.redis.net.front.rewrite.impl.FristSecondKey;
import com.feeyo.redis.net.front.rewrite.impl.MKey;
import com.feeyo.redis.net.front.rewrite.impl.NoKey;
import com.feeyo.redis.net.front.rewrite.impl.SecondKey;

public class KeyRewriteStrategySet {
	
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
	
	private static ThreadLocal<EvalKey> evalKey = new ThreadLocal<EvalKey>() {
		@Override
		protected EvalKey initialValue() {
			return new EvalKey();
		}
	};
	
	static {
		//
		keyStrategys.put("CLUSTER", KeyRewriteStrategy.NoKey);
		keyStrategys.put("INFO", KeyRewriteStrategy.NoKey);
		keyStrategys.put("TIME", KeyRewriteStrategy.NoKey);
		keyStrategys.put("CLIENT", KeyRewriteStrategy.NoKey);
		keyStrategys.put("CONFIG", KeyRewriteStrategy.NoKey);
		keyStrategys.put("DEBUG", KeyRewriteStrategy.NoKey);
		keyStrategys.put("SCRIPT", KeyRewriteStrategy.NoKey);
		
		// 消息订阅
		keyStrategys.put("SUBSCRIBE", KeyRewriteStrategy.NoKey);		//old AllKey
		keyStrategys.put("PSUBSCRIBE", KeyRewriteStrategy.NoKey);
		keyStrategys.put("PUBLISH", KeyRewriteStrategy.NoKey);
		keyStrategys.put("UNSUBSCRIBE", KeyRewriteStrategy.NoKey);
		keyStrategys.put("PUNSUBSCRIBE", KeyRewriteStrategy.NoKey);
		
		// 事务
		keyStrategys.put("MULTI", KeyRewriteStrategy.NoKey);
		keyStrategys.put("EXEC", KeyRewriteStrategy.NoKey);
		keyStrategys.put("DISCARD", KeyRewriteStrategy.NoKey);
		keyStrategys.put("WATCH", KeyRewriteStrategy.AllKey);
		//keyStrategys.put("UNWATCH", KeyPrefixStrategy.AllKey);
		
		
		keyStrategys.put("BRPOP", KeyRewriteStrategy.ExceptLastKey);
		keyStrategys.put("BLPOP", KeyRewriteStrategy.ExceptLastKey);
		keyStrategys.put("BITOP", KeyRewriteStrategy.ExceptFirstKey);
		
		keyStrategys.put("SMOVE", KeyRewriteStrategy.FristSecondKey);
		keyStrategys.put("RPOPLPUSH", KeyRewriteStrategy.FristSecondKey);
		keyStrategys.put("BRPOPLPUSH", KeyRewriteStrategy.FristSecondKey);
		
		keyStrategys.put("EXISTS", KeyRewriteStrategy.AllKey);
		keyStrategys.put("PFMERGE", KeyRewriteStrategy.AllKey);
		keyStrategys.put("PFCOUNT", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SDIFFSTORE", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SDIFF", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SINTERSTORE", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SINTER", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SUNIONSTORE", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SUNION", KeyRewriteStrategy.AllKey);
		keyStrategys.put("SCAN", KeyRewriteStrategy.NoKey);
		
		keyStrategys.put("DEL", KeyRewriteStrategy.AllKey);
		keyStrategys.put("RENAMENX", KeyRewriteStrategy.AllKey);
		keyStrategys.put("RENAME", KeyRewriteStrategy.AllKey);
		
		keyStrategys.put("MGET", KeyRewriteStrategy.AllKey);		
		keyStrategys.put("MSET", KeyRewriteStrategy.MKey);
		keyStrategys.put("MSETNX", KeyRewriteStrategy.MKey);		
		keyStrategys.put("OBJECT", KeyRewriteStrategy.SecondKey);
		
		// eval
		keyStrategys.put("EVAL",  	KeyRewriteStrategy.EvalKey);
		
		// Kafka nokey
		keyStrategys.put("KPUSH", 			KeyRewriteStrategy.NoKey);
		keyStrategys.put("KPOP",  			KeyRewriteStrategy.NoKey);
		keyStrategys.put("KPARTITIONS", 	KeyRewriteStrategy.NoKey);
		keyStrategys.put("KOFFSET",  	    KeyRewriteStrategy.NoKey);
		keyStrategys.put("KGETOFFSET",  	KeyRewriteStrategy.NoKey);
		keyStrategys.put("KRETURNOFFSET",  	KeyRewriteStrategy.NoKey);
	}
	
	// 
	// TODO: 修复一个并发问题，不采用锁，直接 new, 耗内存哦，采用ThreadLocal
	public static KeyRewriteStrategy getStrategy(String cmd) {		
		
		Integer strategyCode = keyStrategys.get( cmd );
		if ( strategyCode == null ) {
			return firstKey.get();
		}
		
		switch( strategyCode ) {
		case KeyRewriteStrategy.AllKey:
			return allKey.get();
		case KeyRewriteStrategy.ExceptFirstKey:
			return exceptFirstKey.get();
		case KeyRewriteStrategy.ExceptLastKey:
			return exceptLastKey.get();
		case KeyRewriteStrategy.FirstKey:
			return firstKey.get();
		case KeyRewriteStrategy.FristSecondKey:
			return fristSecondKey.get();
		case KeyRewriteStrategy.MKey:
			return mKey.get();
		case KeyRewriteStrategy.NoKey:
			return noKey.get();
		case KeyRewriteStrategy.SecondKey:
			return secondKey.get();
		case KeyRewriteStrategy.EvalKey:
			return evalKey.get();
		default:
			return firstKey.get();
		}
	}

}
