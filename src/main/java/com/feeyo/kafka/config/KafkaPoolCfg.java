package com.feeyo.kafka.config;

import java.util.Map;

import com.feeyo.redis.config.PoolCfg;

public class KafkaPoolCfg extends PoolCfg {
	
	// topicName -> topicCfg
	private Map<String, TopicCfg> topicCfgMap = null;

	public KafkaPoolCfg(int id, String name, int type, int minCon, int maxCon) {
		super(id, name, type, minCon, maxCon);
	}
	
	//
	public boolean load() {
		
		return true;
	}
	
	public boolean reload() {
		
		return true;
	}
	
	

}
