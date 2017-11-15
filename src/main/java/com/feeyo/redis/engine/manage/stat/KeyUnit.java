package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.atomic.AtomicLong;

public class KeyUnit {
	public enum KeyType{
		HASH("hash"),LIST("list"),SET("set"),ZSET("zset"),UNHANDLE("unhandle");
		
		private KeyType(String value) {
			this.value = value;
		}
		private final String value;
		public String getValue() {
			return value;
		}
	}
	
	private final String cmd;
	private final String key;
	private final KeyType type;
	private final TopHundredCollectMsg collectMsg;
	private AtomicLong length;
	private AtomicLong count_1k;
	private AtomicLong count_10k;
	
	public KeyUnit(String cmd, String key, KeyType type, TopHundredCollectMsg collectMsg) {
		this.cmd = cmd;
		this.key = key;
		this.type = type;
		this.collectMsg = collectMsg;
	}
	
	public KeyUnit(String cmd, String key, KeyType type, long length, long count_1k, long count_10k, TopHundredCollectMsg collectMsg) {
		super();
		this.cmd = cmd;
		this.key = key;
		this.type = type;
		this.collectMsg = collectMsg;
		this.length = new AtomicLong(length);
		this.count_1k = new AtomicLong(count_1k);
		this.count_10k = new AtomicLong(count_10k);
	}

	public String getKey() {
		return key;
	}

	public KeyType getType() {
		return type;
	}

	public AtomicLong getLength() {
		return length;
	}

	public AtomicLong getCount_1k() {
		return count_1k;
	}

	public AtomicLong getCount_10k() {
		return count_10k;
	}

	public String getCmd() {
		return cmd;
	}

	public TopHundredCollectMsg getCollectMsg() {
		return collectMsg;
	}
	
}