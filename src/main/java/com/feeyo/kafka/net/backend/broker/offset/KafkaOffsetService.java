package com.feeyo.kafka.net.backend.broker.offset;

import java.io.IOException;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

public class KafkaOffsetService {

	private static KafkaOffsetService INSTANCE = new KafkaOffsetService();

	public static KafkaOffsetService INSTANCE() {
		return INSTANCE;
	}
	
	public void start() throws IOException {
		RunningServerAdmin.INSTANCE().start();
	}
	
	public void close() {
		RunningServerAdmin.INSTANCE().stop();
	}
	
	// slave 节点从master上获取 offset
	public long getOffsetForSlave(String user, String topic, int partition) {
		// 如果本机不是master,说明网络异常，目前没有master
		if (!RunningServerAdmin.INSTANCE().isMaster()) {
			return -1;
		}
		UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
		if (userCfg == null) {
			return -1;
		}
		KafkaPoolCfg kafkaPoolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get(userCfg.getPoolId());
		if (kafkaPoolCfg == null) {
			return -1;
		}
		TopicCfg topicCfg = kafkaPoolCfg.getTopicCfgMap().get(topic);
		if (topicCfg == null) {
			return -1;
		}
		
		return RunningOffsetAdmin.INSTANCE().getOffset(topicCfg, user, partition);
	}
	
	// 获取offset
	public long getOffset(String user, TopicCfg topicCfg, int partition) {
		long offset;
		if (RunningServerAdmin.INSTANCE().isMaster()) {
			offset = RunningOffsetAdmin.INSTANCE().getOffset(topicCfg, user, partition);
		} else {
			offset = RemoteOffsetAdmin.INSTANCE().getOffset(user, topicCfg.getName(), partition);
		}
		return offset;
	}

	// 回收 offset
	public void rollbackConsumerOffset(String user, String topic, int partition, long offset) {
		if (RunningServerAdmin.INSTANCE().isMaster()) {
			RunningOffsetAdmin.INSTANCE().rollbackConsumerOffset(user, topic, partition, offset);
		} else {
			RemoteOffsetAdmin.INSTANCE().rollbackConsumerOffset(user, topic, partition, offset);
		}
	}
	
	// 回滚slave节点上的offset
	public void rollbackConsumerOffsetForSlave(String user, String topic, int partition, long offset) {
		if (RunningServerAdmin.INSTANCE().isMaster()) {
			RunningOffsetAdmin.INSTANCE().rollbackConsumerOffset(user, topic, partition, offset);
		}
	}

	// 更新生产offset
	public void updateProducerOffset(String user, String topic, int partition, long offset, long logStartOffset) {
		// TODO 因为影响很小，所以为了减少slave master之间的调用，对slave节点不更新生产点位
		if (RunningServerAdmin.INSTANCE().isMaster()) {
			RunningOffsetAdmin.INSTANCE().updateProducerOffset(user, topic, partition, offset, logStartOffset);
		}
	}
}
