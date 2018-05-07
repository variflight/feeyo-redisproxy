package com.feeyo.kafka.config.loader;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.OffsetManageCfg;
import com.feeyo.redis.config.PoolCfg;

public class KafkaConfigLoader {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaConfigLoader.class );
	
	public static Map<String, TopicCfg> loadTopicCfgMap(Map<Integer, PoolCfg> poolMap, String uri) throws Exception {
		
		Map<String, TopicCfg> map = new HashMap<String, TopicCfg>();
		try {
			NodeList nodeList = loadXmlDoc(uri).getElementsByTagName("property");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				NamedNodeMap nameNodeMap = node.getAttributes();		
				String topic = getAttribute(nameNodeMap, "name", null);
				if (topic == null) {
					LOGGER.warn("kafka topic null...please check kafka.xml...");
					continue;
				}
				int poolId = getIntAttribute(nameNodeMap, "poolId", -1);
				int partitions = getIntAttribute(nameNodeMap, "partitions", 1);
				short replicationFactor = getShortAttribute(nameNodeMap, "replicationFactor", (short)0);
				String[] producers = getAttribute(nameNodeMap, "producer", "").split(",");
				String[] consumers = getAttribute(nameNodeMap, "consumer", "").split(",");
				
				PoolCfg poolCfg = poolMap.get(poolId);
				if ( poolCfg.getType() != 3 ) {
					LOGGER.warn("topic:{} is not a kafka pool...please check kafka.xml...", topic);
					continue;
				}
				TopicCfg kafkaCfg = new TopicCfg(topic, poolId, partitions, replicationFactor, producers, consumers);
				
				map.put(topic, kafkaCfg);
			}
		} catch (Exception e) {
			LOGGER.error("load kafka.xml err: " + e);
			throw e;
		}
		return map;
	}
	
	
	
	public static OffsetManageCfg loadOffsetManageCfg(String uri) {
		OffsetManageCfg offsetCfg = null;
		try {
			NodeList nodeList = loadXmlDoc(uri).getElementsByTagName("offset");
			if (nodeList.getLength() != 1) {
				throw new Exception("kafka offset configure error...");
			}
			Node node = nodeList.item(0);
			NamedNodeMap nameNodeMap = node.getAttributes();
			String server = getAttribute(nameNodeMap, "server", null);
			String path = getAttribute(nameNodeMap, "path", "/root/redis-proxy/kafka/data/topic");
			int index = path.lastIndexOf('/');
			if (index > 0 && index == path.length() - 1) {
				path = path.substring(0, index);
			}
			offsetCfg = new OffsetManageCfg(server, path);
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return offsetCfg;
	}
	
	static Document loadXmlDoc(String uri) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(uri);
		return doc;
	}

	static String getAttribute(NamedNodeMap map, String attr, String defaultVal) {
		return getValue(map.getNamedItem(attr), defaultVal);
	}

	static int getIntAttribute(NamedNodeMap map, String attr, int defaultVal) {
		return getIntValue(map.getNamedItem(attr), defaultVal);
	}
	
	static short getShortAttribute(NamedNodeMap map, String attr, short defaultVal) {
		return getShortValue(map.getNamedItem(attr), defaultVal);
	}
	
	static boolean getBooleanAttribute(NamedNodeMap map, String attr, boolean defaultVal) {
		return getBooleanValue(map.getNamedItem(attr), defaultVal);
	}


	static String getValue(Node node, String defaultVal) {
		return node == null ? defaultVal : node.getNodeValue();
	}

	static int getIntValue(Node node, int defaultVal) {
		return node == null ? defaultVal : Integer.valueOf(node.getNodeValue());
	}
	
	static short getShortValue(Node node, short defaultVal) {
		return node == null ? defaultVal : Short.valueOf(node.getNodeValue());
	}
	
	static boolean getBooleanValue(Node node, boolean defaultVal) {
		return node == null ? defaultVal : Boolean.valueOf(node.getNodeValue());
	}

	static List<Node> getChildNodes(Node theNode, String childElName) {
		LinkedList<Node> nodes = new LinkedList<Node>();
		NodeList childs = theNode.getChildNodes();
		for (int j = 0; j < childs.getLength(); j++) {
			if (childs.item(j).getNodeType() == Document.ELEMENT_NODE && childs.item(j).getNodeName().equals(childElName)) {
				nodes.add(childs.item(j));
			}
		}
		return nodes;
	}
	
}
