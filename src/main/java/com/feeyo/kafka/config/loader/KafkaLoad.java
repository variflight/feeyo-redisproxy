package com.feeyo.kafka.config.loader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.admin.KafkaAdmin;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.config.KafkaCfg;
import com.feeyo.kafka.config.MetaData;
import com.feeyo.kafka.config.MetaDataNode;
import com.feeyo.kafka.config.MetaDataOffset;
import com.feeyo.kafka.config.MetaDataPartition;
import com.feeyo.kafka.net.backend.callback.ApiVersionCallback;
import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.util.Utils;
import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.nio.NetSystem;

public class KafkaLoad {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaLoad.class);

	private final static KafkaLoad instance = new KafkaLoad();
	private ReentrantLock lock = new ReentrantLock();

	public static KafkaLoad instance() {
		return instance;
	}

	public void load(Map<String, KafkaCfg> kafkaMap) {
		Map<Integer, List<KafkaCfg>> topics = groupBy(kafkaMap);
		for (Entry<Integer, List<KafkaCfg>> entry : topics.entrySet()) {
			// 获取kafka地址
			int poolId = entry.getKey();
			PoolCfg poolCfg = RedisEngineCtx.INSTANCE().getPoolCfgMap().get(poolId);
			StringBuffer servers = new StringBuffer();
			List<String> nodes = poolCfg.getNodes();
			for (int i = 0; i < nodes.size(); i++) {
				String str = nodes.get(i);
				String[] node = str.split(":");
				servers.append(node[0]).append(":").append(node[1]);
				if (i < nodes.size() - 1) {
					servers.append(",");
				}
			}

			// 获取kafka管理对象
			KafkaAdmin kafkaAdmin = new KafkaAdmin(servers.toString());
			Map<String, TopicDescription> existsTopics = kafkaAdmin.getTopicAndDescriptions();
			List<KafkaCfg> kafkaCfgs = entry.getValue();
			for (KafkaCfg kafkaCfg : kafkaCfgs) {
				if (existsTopics.containsKey(kafkaCfg.getTopic())) {
					TopicDescription topicDescription = existsTopics.get(kafkaCfg.getTopic());
					List<TopicPartitionInfo> partitions = topicDescription.partitions();
					// 如果增加分区
					if (partitions.size() < kafkaCfg.getPartitions()) {
						kafkaAdmin.addPartitionsForTopic(kafkaCfg.getTopic(), kafkaCfg.getPartitions());
						topicDescription = kafkaAdmin.getDescriptionByTopic(kafkaCfg.getTopic());
					}

					initKafkaCfgMetaData(kafkaCfg, topicDescription);
				} else {
					kafkaAdmin.createTopic(kafkaCfg.getTopic(), kafkaCfg.getPartitions(),
							kafkaCfg.getReplicationFactor());
					TopicDescription topicDescription = kafkaAdmin.getDescriptionByTopic(kafkaCfg.getTopic());
					// 初始化metadata
					initKafkaCfgMetaData(kafkaCfg, topicDescription);
				}
			}

			kafkaAdmin.close();
		}

	}

	/**
	 * 初始化kafka配置。metadata
	 * 
	 * @param kafkaCfg
	 * @param topicDescription
	 */
	private void initKafkaCfgMetaData(KafkaCfg kafkaCfg, TopicDescription topicDescription) {
		if (topicDescription == null) {
			kafkaCfg.setMetaData(null);
			return;
		}
		List<TopicPartitionInfo> partitions = topicDescription.partitions();
		MetaDataPartition[] metaDataPartitions = new MetaDataPartition[partitions.size()];
		MetaData metaData = new MetaData(topicDescription.name(), topicDescription.isInternal(), metaDataPartitions);

		int id = -1;
		for (int i = 0; i < partitions.size(); i++) {
			TopicPartitionInfo topicPartitionInfo = partitions.get(i);
			List<Node> replicas = topicPartitionInfo.replicas();
			MetaDataNode[] replicasData = new MetaDataNode[replicas.size()];
			for (int j = 0; j < replicas.size(); j++) {
				replicasData[j] = new MetaDataNode(replicas.get(j).id(), replicas.get(j).host(),
						replicas.get(j).port());
			}
			MetaDataNode leader = new MetaDataNode(topicPartitionInfo.leader().id(), topicPartitionInfo.leader().host(),
					topicPartitionInfo.leader().port());

			MetaDataPartition mdp = new MetaDataPartition(topicPartitionInfo.partition(), leader, replicasData);
			metaDataPartitions[i] = mdp;
			id = leader.getId();
		}

		loadApiVersion(kafkaCfg, id);

		kafkaCfg.setMetaData(metaData);
	}

	/**
	 * 加载apiversion
	 * @param kafkaCfg
	 * @param id
	 */
	private void loadApiVersion(KafkaCfg kafkaCfg, int id) {
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get(kafkaCfg.getPoolId());
		PhysicalNode physicalNode = pool.getPhysicalNode(id);
		if (physicalNode != null) {
			try {
				
				RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS.id, (short)1, 
						Thread.currentThread().getName(), Utils.getCorrelationId());
				
				Struct struct = requestHeader.toStruct();
				final ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( struct.sizeOf() + 4 );
				buffer.putInt(struct.sizeOf());
				struct.writeTo(buffer);
				
				//
				AbstractBackendCallback callback = new ApiVersionCallback();
				
				BackendConnection backendCon = physicalNode.getConnection(callback, null);
				if ( backendCon == null ) {
					TodoTask task = new TodoTask() {				
						@Override
						public void execute(BackendConnection backendCon) throws Exception {	
							backendCon.write( buffer );
						}
					};
					callback.addTodoTask(task);
					backendCon = physicalNode.createNewConnection(callback, null);
					
				} else {
					backendCon.write(buffer);
				}
			} catch (IOException e) {
				LOGGER.warn("", e);
			}
		}
	}

	private Map<Integer, List<KafkaCfg>> groupBy(Map<String, KafkaCfg> kafkaMap) {
		Map<Integer, List<KafkaCfg>> topics = new HashMap<Integer, List<KafkaCfg>>();
		for (Entry<String, KafkaCfg> entry : kafkaMap.entrySet()) {
			KafkaCfg kafkaCfg = entry.getValue();
			int poolId = kafkaCfg.getPoolId();
			List<KafkaCfg> kafkaCfgs = topics.get(poolId);
			if (kafkaCfgs == null) {
				kafkaCfgs = new ArrayList<KafkaCfg>();
				topics.put(poolId, kafkaCfgs);
			}
			kafkaCfgs.add(kafkaCfg);
		}
		return topics;
	}

	// 重新加载
	public byte[] reLoad() {
		final ReentrantLock lock = this.lock;
		lock.lock();

		Map<Integer, PoolCfg> poolCfgMap = RedisEngineCtx.INSTANCE().getPoolCfgMap();
		Map<String, KafkaCfg> kafkaMap = RedisEngineCtx.INSTANCE().getKafkaMap();
		try {
			// 重新加载kafkamap
			Map<String, KafkaCfg> newKafkaMap = KafkaConfigLoader.loadKafkaMap(poolCfgMap,
					ConfigLoader.buidCfgAbsPathFor("kafka.xml"));
			load(newKafkaMap);

			for (Entry<String, KafkaCfg> entry : newKafkaMap.entrySet()) {
				String key = entry.getKey();
				KafkaCfg newKafkaCfg = entry.getValue();
				KafkaCfg oldKafkaCfg = kafkaMap.get(key);
				if (oldKafkaCfg != null) {
					// 迁移原来的offset
					newKafkaCfg.getMetaData().setOffsets(oldKafkaCfg.getMetaData().getOffsets());

					// 新建的topic
				} else {
					Map<Integer, MetaDataOffset> metaDataOffsets = new ConcurrentHashMap<Integer, MetaDataOffset>();

					for (MetaDataPartition partition : newKafkaCfg.getMetaData().getPartitions()) {
						MetaDataOffset metaDataOffset = new MetaDataOffset(partition.getPartition(), 0);
						metaDataOffsets.put(partition.getPartition(), metaDataOffset);
					}

					newKafkaCfg.getMetaData().setOffsets(metaDataOffsets);
				}
			}
			RedisEngineCtx.INSTANCE().setKafkaMap(newKafkaMap);
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		} finally {
			lock.unlock();
		}

		return "+OK\r\n".getBytes();
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090,localhost:9091,localhost:9092");
		AdminClient adminClient = AdminClient.create(props);

		// 创建topic
		// List<NewTopic> newTopics = new ArrayList<NewTopic>();
		// NewTopic topic = new NewTopic("test2", 1, (short) 1);
		// newTopics.add(topic);
		// CreateTopicsOptions cto = new CreateTopicsOptions();
		// cto.timeoutMs(2000);
		// CreateTopicsResult crt = adminClient.createTopics(newTopics, cto);
		// System.out.println(crt.all().get());

		// 查询topic
		ListTopicsResult ss = adminClient.listTopics();
		// 删除topic
		// adminClient.deleteTopics(ss.names().get());
		// 查询topic配置信息
		DescribeTopicsResult dtr = adminClient.describeTopics(ss.names().get());
		KafkaFuture<Map<String, TopicListing>> a = ss.namesToListings();

		System.out.println(a.get());
		System.out.println(dtr.all().get());
		System.out.println(dtr.all().get().containsKey("test"));
		TopicDescription topicDescription = dtr.all().get().get("test");
		List<TopicPartitionInfo> list = topicDescription.partitions();
		System.out.println(topicDescription);
		System.out.println(list.size());

		// 给topic增加分区数，只能增加不能减少
		Map<String, NewPartitions> map = new HashMap<>();
		NewPartitions x = NewPartitions.increaseTo(1);
		map.put("test2", x);
		// CreatePartitionsResult cpr = adminClient.createPartitions(map);

		// System.out.println(cpr.all().get());

		DescribeClusterOptions dco = new DescribeClusterOptions();
		dco.timeoutMs(5 * 1000);
		DescribeClusterResult dcr = adminClient.describeCluster(dco);
		System.out.println(dcr.nodes());
		System.out.println(dcr.nodes().get());
	}
}
