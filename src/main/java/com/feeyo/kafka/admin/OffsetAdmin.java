package com.feeyo.kafka.admin;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.feeyo.kafka.config.ConsumerOffset;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.DataOffset;
import com.feeyo.kafka.config.DataPartition;
import com.feeyo.kafka.config.OffsetManageCfg;
import com.feeyo.kafka.config.loader.KafkaConfigLoader;
import com.feeyo.kafka.util.JsonUtils;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.engine.RedisEngineCtx;

/**
 * 管理 topic offset
 * 
 * @author yangtao
 */
public class OffsetAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger(OffsetAdmin.class);
	
	private static final String ZK_CFG_FILE = "kafka.xml"; // zk settings is in server.xml
	

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	private static OffsetAdmin INSTANCE = new OffsetAdmin();

	private CuratorFramework curator;
	private OffsetManageCfg offsetManageCfg;
	
	
	public static OffsetAdmin getInstance() {
		return INSTANCE;
	}
	
	private OffsetAdmin() {

		offsetManageCfg = KafkaConfigLoader.loadOffsetManageCfg(ConfigLoader.buidCfgAbsPathFor(ZK_CFG_FILE));
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(offsetManageCfg.getServer())
				.retryPolicy(new RetryNTimes(3, 1000)).connectionTimeoutMs(3000);

		curator = builder.build();

		curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState state) {
				switch (state) {
				case CONNECTED:
					LOGGER.info("connected with zookeeper");
					break;
				case LOST:
					LOGGER.warn("lost session with zookeeper");
					break;
				case RECONNECTED:
					LOGGER.warn("reconnected with zookeeper");
					break;
				default:
					break;
				}
			}
		});
		curator.start();
		
		INSTANCE = this;
	}
	
	/**
	 * 创建节点
	 * @param path
	 */
	private void createZkNode(String path, byte[] data) {
		try {
			if (curator.checkExists().forPath(path) != null) {
				return;
			}
			int index = path.lastIndexOf('/');
			if (index > 0) {
				createZkNode(path.substring(0, index), null);
			}
			curator.create().forPath(path, data);
		} catch (Exception e) {
			LOGGER.warn("", e);
		}
	}
	
	/**
	 * 加载offset
	 */
	@SuppressWarnings("unchecked")
	private void load() {
		Map<String, TopicCfg> topicCfgMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
		
		for (Entry<String, TopicCfg> entry : topicCfgMap.entrySet()) {
			TopicCfg topicCfg = entry.getValue();
			String topicName  = topicCfg.getName();
			String path = offsetManageCfg.getPath() + File.separator + topicName;
			try {
				// base node 
				createZkNode(path, null);
				
				Map<Integer, DataOffset> dataOffsets = new ConcurrentHashMap<Integer, DataOffset>();
				byte[] data = curator.getData().forPath(path);
				if (data == null) {
					
					for (DataPartition partition : topicCfg.getMetadata().getPartitions()) {
						DataOffset dataOffset = new DataOffset(partition.getPartition(), 0, 0);
						dataOffsets.put(partition.getPartition(), dataOffset);
					}
					saveOffsetsToZk(topicName, dataOffsets);
					
				} else {
					
					/*
					 {
					 	2:{"offsets":{"pwd01":{"consumer":"pwd01","offset":1},"pwd02":{"consumer":"pwd02","offset":2}},"partition":1,"producerOffset":100}
					  	1:{"offsets":{"pwd01":{"consumer":"pwd01","offset":1},"pwd02":{"consumer":"pwd02","offset":2}},"partition":1,"producerOffset":100}
					  }
					*/

					String str = new String(data);
					JSONObject obj = JsonUtils.unmarshalFromString(str, JSONObject.class);
					
					for (DataPartition partition : topicCfg.getMetadata().getPartitions()) {

						Object metaDataOffsetObject = obj.get( String.valueOf(partition.getPartition()) );
						if (metaDataOffsetObject == null) {
							DataOffset dataOffset = new DataOffset(partition.getPartition(), 0, 0);
							dataOffsets.put(partition.getPartition(), dataOffset);
							
						} else {
							JSONObject jsonObject = JsonUtils.unmarshalFromString(String.valueOf(metaDataOffsetObject), JSONObject.class);
							
							DataOffset dataOffset = new DataOffset(partition.getPartition(),
									jsonObject.get("producerOffset") == null ? 0 : Integer.parseInt(jsonObject.getString("producerOffset")),
									jsonObject.get("logStartOffset") == null ? 0 : Integer.parseInt(jsonObject.getString("logStartOffset")));
							
							Map<String, ConsumerOffset> consumerOffsets = new ConcurrentHashMap<String, ConsumerOffset>();
							
							JSONObject offsetsObject = JsonUtils.unmarshalFromString(jsonObject.getString("offsets"), JSONObject.class);
							HashSet<String> consumers = topicCfg.getConsumers();
							for (String consumer : consumers) {

								if (offsetsObject.get(consumer) != null) {
									
									JSONObject consumeJson = JsonUtils.unmarshalFromString(offsetsObject.getString(consumer), JSONObject.class);
									ConsumerOffset cOffset = new ConsumerOffset(
											 consumer, consumeJson.getString("offset") == null ? 0 
											: Integer.parseInt(consumeJson.getString("offset")));
									if (consumeJson.get("defaultOffset") != null) {
										List<Object> defaultOffsets = JsonUtils.unmarshalFromString(consumeJson.getString("defaultOffset"), List.class);
										for (Object defaultOffset : defaultOffsets) {
											cOffset.offerOffset(Long.parseLong(String.valueOf(defaultOffset)));
										}
									}
									
									consumerOffsets.put(consumer, cOffset);
									
								} else {
									ConsumerOffset cOffset = new ConsumerOffset(consumer, 0);
									consumerOffsets.put(consumer, cOffset);
								}
							}
							
							dataOffset.setConsumerOffsets(consumerOffsets);
							
							dataOffsets.put(partition.getPartition(), dataOffset);
						}
						
					}
					
				}
				
				topicCfg.getMetadata().setDataOffsets(dataOffsets);
				
			} catch (Exception e) {
				LOGGER.warn("", e);
			}
		}
		
	}
	
	private void saveOffsetsToZk(String topicName,  Map<Integer, DataOffset> offset) {
		String path = offsetManageCfg.getPath() + File.separator + topicName;
		Stat stat;
		try {
			stat = curator.checkExists().forPath(path);
			if (stat == null) {
				ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
			}
			
			curator.setData().inBackground().forPath(path, JsonUtils.marshalToByte(offset));
		} catch (Exception e) {
			LOGGER.warn("kafka cmd offset commit err:", e);
		}
	}

	public void startUp() {
		
		this.load();
		
		// 定时持久化offset
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					// offset 数据持久化
					Map<String, TopicCfg> topicCfgMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
					for (Entry<String, TopicCfg> entry : topicCfgMap.entrySet()) {
						TopicCfg topicCfg = entry.getValue();
						saveOffsetsToZk(topicCfg.getName(), topicCfg.getMetadata().getDataOffsets());
					}
				} catch (Exception e) {
					LOGGER.warn("offsetAdmin err: ", e);
				}
				
			}
		}, 30, 30, TimeUnit.SECONDS);
	}
	
	public void close() {
		
		// 关闭定时任务
		executorService.shutdown();
		
		// 停止获取新的offset 
		Map<String, TopicCfg> kafkaTopicMap = RedisEngineCtx.INSTANCE().getKafkaTopicMap();
		for (Entry<String, TopicCfg> entry : kafkaTopicMap.entrySet()) {
			TopicCfg topicCfg = entry.getValue();
			topicCfg.getMetadata().close();
		}
		
		// 提交本地剩余offset
		for (Entry<String, TopicCfg> entry : kafkaTopicMap.entrySet()) {
			TopicCfg topicCfg = entry.getValue();
			saveOffsetsToZk(topicCfg.getName(), topicCfg.getMetadata().getDataOffsets());
		}
	}
}
