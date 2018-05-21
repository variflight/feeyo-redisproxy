package com.feeyo.kafka.net.backend.broker.running;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.OffsetManageCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.BrokerPartitionOffset;
import com.feeyo.kafka.net.backend.broker.ConsumerOffset;
import com.feeyo.kafka.net.backend.broker.running.zk.ZkClientx;
import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;

/**
 * 管理 topic offset
 * 
 * @author yangtao
 */
public class RunningOffsetAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger(RunningOffsetAdmin.class);
	
	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	private static RunningOffsetAdmin INSTANCE = new RunningOffsetAdmin();

	private OffsetManageCfg offsetManageCfg;
	
	private boolean isRunning = false;
	
	public static RunningOffsetAdmin INSTANCE() {
		return INSTANCE;
	}
	
	private RunningOffsetAdmin() {}
	
	private void savePartitionOffsets(String topicName,  Map<Integer, BrokerPartitionOffset> partitionOffsetMap, int poolId) {
		
		String basepath = offsetManageCfg.getOffsetPath() + File.separator + String.valueOf(poolId) + File.separator + topicName;
		
		ZkClientx zkclientx = ZkClientx.getZkClient(offsetManageCfg.getServer());
		try {
			for (Entry<Integer, BrokerPartitionOffset> entry : partitionOffsetMap.entrySet()) {
				
				// 点位
				BrokerPartitionOffset partitionOffset = entry.getValue();
				String path = basepath + File.separator + entry.getKey();
				if (!zkclientx.exists(path)) {
					zkclientx.createPersistentSequential(path, true);
				}
				zkclientx.writeData(path, JsonUtils.marshalToByte( partitionOffset ));
				
				// 消费者点位
				Map<String, ConsumerOffset> consumerOffsets = partitionOffset.getConsumerOffsets();
				for (Entry<String, ConsumerOffset> consumerOffsetEntry : consumerOffsets.entrySet()) {
					
					String consumerOffsetPath = path + File.separator + consumerOffsetEntry.getKey();
					if (!zkclientx.exists(consumerOffsetPath)) {
						zkclientx.createPersistentSequential(consumerOffsetPath, true);
					}
					
					ConsumerOffset co = consumerOffsetEntry.getValue();
					zkclientx.writeData(consumerOffsetPath, JsonUtils.marshalToByte(co));
				}
			}
		} catch (Exception e) {
			LOGGER.warn("kafka cmd offset commit err:", e);
		}
	}

	
	
	private void loadPartitionOffsetByPoolId(Map<String, TopicCfg> topicCfgMap, int poolId) {
		ZkClientx zkclientx = ZkClientx.getZkClient(offsetManageCfg.getServer());
		for (TopicCfg topicCfg : topicCfgMap.values()) {
			
			String topicName  = topicCfg.getName();
			String basepath = offsetManageCfg.getOffsetPath() + File.separator  + String.valueOf(poolId) + File.separator + topicName;
			
			Map<Integer, BrokerPartitionOffset> partitionOffsetMap = new ConcurrentHashMap<Integer, BrokerPartitionOffset>();
			try {
				for (BrokerPartition partition : topicCfg.getRunningOffset().getBrokerPartitions()) {
					
					String path = basepath + File.separator + partition.getPartition();
					// base node 
					if (!zkclientx.exists(path)) {
						zkclientx.createPersistentSequential(path, true);
					}
					
					String data;
					Object obj = zkclientx.readData( path );
					if ( obj instanceof String ) {
						data = (String)obj;
					} else {
						data = new String((byte[])obj);
					}
					
					if (isNull(data)) {
						
						BrokerPartitionOffset partitionOffset = new BrokerPartitionOffset(partition.getPartition(), 0, 0);
						partitionOffsetMap.put(partition.getPartition(), partitionOffset);
						
					} else {
						// {"logStartOffset":0,"partition":0,"producerOffset":0}
						BrokerPartitionOffset partitionOffset = JsonUtils.unmarshalFromString(data, BrokerPartitionOffset.class);
						partitionOffsetMap.put(partition.getPartition(), partitionOffset);
						
						List<String> childrenPath = zkclientx.getChildren(path);
						for (String clildPath : childrenPath) {
							
							String consumerOffset;
							Object object = zkclientx.readData( path + File.separator + clildPath );
							if ( object instanceof String ) {
								consumerOffset = (String)object;
							} else {
								consumerOffset = new String((byte[])object);
							}
							
							if (isNull(consumerOffset)) {
								continue;
							}
							ConsumerOffset co = JsonUtils.unmarshalFromString(consumerOffset, ConsumerOffset.class);
							partitionOffset.getConsumerOffsets().put(co.getConsumer(), co);
						}
					}
				}
				
				topicCfg.getRunningOffset().setPartitionOffsets( partitionOffsetMap );
				
			} catch (Exception e) {
				LOGGER.warn("", e);
			}
		}
	}
	
	public void startup(OffsetManageCfg offsetManageCfg) {
		if (isRunning) {
			return;
		}
		
		this.offsetManageCfg = offsetManageCfg;
		
		final Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		for (Entry<Integer, PoolCfg> entry : poolCfgMap.entrySet()) {
			PoolCfg poolCfg = entry.getValue();
			if (poolCfg instanceof KafkaPoolCfg) {
				Map<String, TopicCfg> topicCfgMap = ((KafkaPoolCfg) poolCfg).getTopicCfgMap();
				
				// 加载offset
				loadPartitionOffsetByPoolId(topicCfgMap, poolCfg.getId());
			}
		}
		
		// 定时持久化offset
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					// offset 数据持久化
					saveAll();
					
				} catch (Exception e) {
					LOGGER.warn("offsetAdmin err: ", e);
				}
				
			}
		}, 30, 30, TimeUnit.SECONDS);
		
		isRunning = true;
	}

	/**
	 * 关闭
	 */
	public void close() {
		if (!isRunning) {
			return;
		}
		
		isRunning = false;
		
		try {
			// 关闭定时任务
			executorService.shutdown();
			
			// 提交本地剩余offset
			saveAll();
		} catch (Exception e) {
			isRunning = true;
		}

	}

	/**
	 * offsets 持久化
	 */
	private void saveAll() {
		final Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		for (Entry<Integer, PoolCfg> poolEntry : poolCfgMap.entrySet()) {
			PoolCfg poolCfg = poolEntry.getValue();
			if (poolCfg instanceof KafkaPoolCfg) {
				Map<String, TopicCfg> topicCfgMap = ((KafkaPoolCfg) poolCfg).getTopicCfgMap();
				
				for (Entry<String, TopicCfg> topicEntry : topicCfgMap.entrySet()) {
					TopicCfg topicCfg = topicEntry.getValue();
					savePartitionOffsets(topicCfg.getName(), topicCfg.getRunningOffset().getPartitionOffsets(), poolCfg.getId());
				}
			}
		}
	}
	
	private boolean isNull(String str) {
		if (str == null) {
			return true;
		}
		
		if ("".equals(str) || "null".equals(str) || "NULL".equals(str))  {
			return true;
		}
		
		return false;
	}
}
