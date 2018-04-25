package com.feeyo.redis.kafka.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdmin {
    private static Logger LOGGER = LoggerFactory.getLogger( KafkaAdmin.class );
	
	private AdminClient adminClient;
	
	public KafkaAdmin(String servers) {
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, servers);
		this.adminClient = AdminClient.create(props); 
	}

	public CreateTopicsResult createTopic(String name, int numPartitions, short replicationFactor) {
		List<NewTopic> newTopics = new ArrayList<NewTopic>();
		NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
		newTopics.add(topic);
		
		CreateTopicsOptions cto = new CreateTopicsOptions();
		cto.timeoutMs(5 * 1000);
		return adminClient.createTopics(newTopics, cto);
	}
	
	/**
	 * 获取所有topic和配置信息
	 * @return
	 */
	public Map<String, TopicDescription> getTopicAndDescriptions() {
		// 查询topic
		ListTopicsOptions lto = new ListTopicsOptions();
		lto.timeoutMs(10 * 1000);
		ListTopicsResult ltr = adminClient.listTopics(lto);
		// 查询topic配置信息
		try {
			DescribeTopicsOptions dto = new DescribeTopicsOptions();
			dto.timeoutMs(15 * 1000);
			DescribeTopicsResult dtr = adminClient.describeTopics(ltr.names().get(), dto);
			return dtr.all().get();
		} catch (InterruptedException e) {
			LOGGER.warn("", e);
		} catch (ExecutionException e) {
			LOGGER.warn("", e);
		}
		return null;
	}
	
	/**
	 * 给topic增加分区
	 * @param topic
	 * @param partitions
	 * @return
	 */
	public CreatePartitionsResult addPartitionsForTopic(String topic, int partitions) {
		Map<String, NewPartitions> map = new HashMap<>();
		NewPartitions np = NewPartitions.increaseTo(partitions);
		map.put(topic, np);
		CreatePartitionsOptions cpo = new CreatePartitionsOptions();
		cpo.timeoutMs(5 * 1000);
		return adminClient.createPartitions(map, cpo);
	}
	
	/**
	 * 获取指定topic的配置信息
	 * @return
	 */
	public TopicDescription getDescriptionByTopic(String topic) {
		List<String> topics = new ArrayList<String>();
		topics.add(topic);
		try {
			DescribeTopicsOptions dto = new DescribeTopicsOptions();
			dto.timeoutMs(5 * 1000);
			DescribeTopicsResult dtr = adminClient.describeTopics(topics, dto);
			return dtr.all().get().get(topic);
		} catch (InterruptedException e) {
			LOGGER.warn("", e);
		} catch (ExecutionException e) {
			LOGGER.warn("", e);
		} catch (Exception e) {
			LOGGER.warn("", e);
		}
		return null;
	}

	/**
	 * 获取kafka集群配置信息
	 * @return
	 */
	public Collection<Node> getClusterNodes() {
		try {
			DescribeClusterOptions dco = new DescribeClusterOptions();
			dco.timeoutMs(5 * 1000);
			DescribeClusterResult dcr = adminClient.describeCluster(dco);
			return dcr.nodes().get();
		} catch (Exception e) {
			return null;
		}
	}
	
	public void close() {
		adminClient.close();
	}
}
