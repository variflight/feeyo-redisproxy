package com.feeyo.redis.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.ConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feeyo.redis.config.kafka.KafkaCfg;
import com.feeyo.redis.config.kafka.OffsetCfg;

public class ConfigLoader {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ConfigLoader.class );
	
	/**
	 * 获取 server.xml
	 * 
	 * @param uri
	 * @return
	 * @throws Exception 
	 */
	public static Map<String, String> loadServerMap(String uri) throws Exception {	
		Map<String, String> map = new HashMap<String, String>();
		try {
			Element element = loadXmlDoc(uri).getDocumentElement();
			NodeList children = element.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				Node node = children.item(i);
				if (node instanceof Element) {
					Element e = (Element) node;
					String name = e.getNodeName();
					if ("property".equals(name)) {
						String key = e.getAttribute("name");
						String value = e.getTextContent();
						map.put(key, value);		
					}
				}
			}

		} catch (Exception e) {
			LOGGER.error("loadServerCfg err " + e);
			throw e;
		}		
		return map;
		
	}
	
	/**
	 *  获取连接池配置
	 * 
	 * @param uri
	 * @return
	 * @throws Exception 
	 */
	public static Map<Integer, PoolCfg> loadPoolMap(String uri) throws Exception {

		Map<Integer, PoolCfg> map = new HashMap<Integer, PoolCfg>();
		
		try {

			NodeList nodesElements = loadXmlDoc( uri ).getElementsByTagName("pool");
			for (int i = 0; i < nodesElements.getLength(); i++) {
				Node nodesElement = nodesElements.item(i);

				NamedNodeMap nameNodeMap = nodesElement.getAttributes();
				int id = getIntAttribute(nameNodeMap, "id", -1);
				String name = getAttribute(nameNodeMap, "name", null);
				int type = getIntAttribute(nameNodeMap, "type", 0);
				int minCon = getIntAttribute(nameNodeMap, "minCon", 5);
				int maxCon = getIntAttribute(nameNodeMap, "maxCon", 100);

				PoolCfg poolCfg = new PoolCfg(id, name, type, minCon, maxCon);
				List<Node> nodeList = getChildNodes(nodesElement, "node");
				for(int j = 0; j < nodeList.size(); j++) {
					Node node = nodeList.get(j);					
					NamedNodeMap attrs = node.getAttributes();
					String ip = getAttribute(attrs, "ip", null);
					int port = getIntAttribute(attrs, "port", 6379);
					String suffix = getAttribute(attrs, "suffix", null);
					if(type == 2 && suffix == null) {
						throw new ConfigurationException("Customer Cluster nodes need to set unique suffix property");
					} else {
						poolCfg.addNode( ip + ":" + port + ":" + suffix);
					}
				}
				map.put(id,  poolCfg);
			}
		} catch (Exception e) {
			LOGGER.error("loadPoolCfg err " + e);
			throw e;
		}
		return map;
	}
	
	public static Map<String, UserCfg> loadUserMap(Map<Integer, PoolCfg> poolMap, String uri) throws Exception  {

		Map<String, UserCfg> map = new HashMap<String, UserCfg>();
		try {
			NodeList nodeList = loadXmlDoc(uri).getElementsByTagName("user");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				NamedNodeMap nameNodeMap = node.getAttributes();				
				int poolId = getIntAttribute(nameNodeMap, "poolId", -1);
				String password = getAttribute(nameNodeMap, "password", null);
				String prefix = getAttribute(nameNodeMap, "prefix", null);
				if ( prefix != null && prefix.trim().equals("") ) {
					prefix = null;
				}
				int selectDb = getIntAttribute(nameNodeMap, "selectDb", -1);
				int isAdmin = getIntAttribute(nameNodeMap, "isAdmin", 0);
				int throughPercentage = getIntAttribute(nameNodeMap, "throughPercentage", 100);
				
				boolean isReadonly = getBooleanAttribute(nameNodeMap, "readonly", false);
					
				PoolCfg poolCfg = poolMap.get(poolId);
				int poolType = poolCfg.getType();
				
				UserCfg userCfg = new UserCfg(poolId, poolType, password, prefix, selectDb, isAdmin == 0 ? false : true, isReadonly, throughPercentage);
				map.put(password, userCfg);
			}
		} catch (Exception e) {
			LOGGER.error("loadUsers err " + e);
			throw e;
		}
		return map;
	}
	
	public static Map<String, KafkaCfg> loadKafkaMap(Map<Integer, PoolCfg> poolMap, String uri) throws Exception {
		
		Map<String, KafkaCfg> map = new HashMap<String, KafkaCfg>();
		try {
			NodeList nodeList = loadXmlDoc(uri).getElementsByTagName("property");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				NamedNodeMap nameNodeMap = node.getAttributes();		
//				<topic name="test" poolId="3" partitions="1" replicationFactor="1" producer="pwd01" consumer="pwd01,pwd02"/>
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
				KafkaCfg kafkaCfg = new KafkaCfg(topic, poolId, partitions, replicationFactor, producers, consumers);
				
				map.put(topic, kafkaCfg);
			}
		} catch (Exception e) {
			LOGGER.error("loadUsers err " + e);
			throw e;
		}
		return map;
	}
	
	//
	public static ZkCfg loadZkCfg(String uri) {
		ZkCfg zkCfg = new ZkCfg();
		try {
			NodeList zks = loadXmlDoc( uri ).getElementsByTagName("zookeeper");
			if (zks.getLength() == 1) {
				NodeList zk = zks.item(0).getChildNodes();

				for (int i = 0; i < zk.getLength(); i++) {
					Node node = zk.item(i);
					if (node instanceof Element) {
						Element e = (Element) node;
						switch (e.getNodeName()) {
							case "usingZk":
								String usingZk = e.getAttribute("value");
								if (usingZk.equalsIgnoreCase("true")) {
									zkCfg.setUsingZk(true);
								} else if (usingZk.equalsIgnoreCase("false")) {
									zkCfg.setUsingZk(false);
								} else {
									throw new Exception("parse usingZk error," + usingZk);
								}
								break;
							case "autoActivation":
								String autoActivation = e.getAttribute("value");

								if (autoActivation.equalsIgnoreCase("true")) {
									zkCfg.setAutoAct(true);
								} else if (autoActivation.equalsIgnoreCase("false")) {
									zkCfg.setAutoAct(false);
								} else {
									throw new Exception("parse autoActivation error," + autoActivation);
								}
								break;
							case "zkHome":
								String zkHome = e.getAttribute("value");
								if (zkHome.matches("^\\/([\\w-]+\\/){1,}$")) {
									zkCfg.setZkHome(e.getAttribute("value"));
								} else {
									throw new Exception("parse zkHome error," + zkHome);
								}
								break;
							case "node":
								String ipAndPort = e.getAttribute("ipAndPort");
								if (ipAndPort.matches("^((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))):\\d+$")) {
									zkCfg.getZkServers().add(ipAndPort);
								} else {
									throw new Exception("parse zk node error," + ipAndPort);
								}
								break;
							case "conf":
								String cfgName = e.getAttribute("name");
								if (cfgName.matches("^[\\w_\\s-]+\\.[A-Za-z]{1,}$")) {
									zkCfg.getLocCfgNames().add(cfgName);
								} else {
									throw new Exception("parse configure file name error," + cfgName);
								}

								break;
							default:
								LOGGER.error("load zookeeper configure error", e);
								return null;
						}
					}
				}
			}
		} catch (Exception e) {
			zkCfg = null;
			LOGGER.error("load zookeeper configure error", e);
		}
		return zkCfg;
	}
	
	public static OffsetCfg loadKafkaOffsetCfg(String uri) {
		OffsetCfg offsetCfg = null;
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
			offsetCfg = new OffsetCfg(server, path);
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return offsetCfg;
	}
	
	/*
	 * load mail properties
	 */
	public static Properties loadMailProperties(String uri) throws Exception {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(uri));
		} catch (Exception e) {
			LOGGER.error("load mail property error", e);
			throw e;
			
		}
		return props;
	}
	
	private static Document loadXmlDoc(String uri) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(uri);
		return doc;
	}

	private static String getAttribute(NamedNodeMap map, String attr, String defaultVal) {
		return getValue(map.getNamedItem(attr), defaultVal);
	}

	private static int getIntAttribute(NamedNodeMap map, String attr, int defaultVal) {
		return getIntValue(map.getNamedItem(attr), defaultVal);
	}
	
	private static short getShortAttribute(NamedNodeMap map, String attr, short defaultVal) {
		return getShortValue(map.getNamedItem(attr), defaultVal);
	}
	
	private static boolean getBooleanAttribute(NamedNodeMap map, String attr, boolean defaultVal) {
		return getBooleanValue(map.getNamedItem(attr), defaultVal);
	}


	private static String getValue(Node node, String defaultVal) {
		return node == null ? defaultVal : node.getNodeValue();
	}

	private static int getIntValue(Node node, int defaultVal) {
		return node == null ? defaultVal : Integer.valueOf(node.getNodeValue());
	}
	
	private static short getShortValue(Node node, short defaultVal) {
		return node == null ? defaultVal : Short.valueOf(node.getNodeValue());
	}
	
	private static boolean getBooleanValue(Node node, boolean defaultVal) {
		return node == null ? defaultVal : Boolean.valueOf(node.getNodeValue());
	}

	private static List<Node> getChildNodes(Node theNode, String childElName) {
		LinkedList<Node> nodes = new LinkedList<Node>();
		NodeList childs = theNode.getChildNodes();
		for (int j = 0; j < childs.getLength(); j++) {
			if (childs.item(j).getNodeType() == Document.ELEMENT_NODE && childs.item(j).getNodeName().equals(childElName)) {
				nodes.add(childs.item(j));
			}
		}
		return nodes;
	}
	
	public static String buidCfgAbsPathFor(String fileName) {
		StringBuffer path = new StringBuffer();
		path.append( System.getProperty("FEEYO_HOME") ).append( File.separator );
		path.append( "conf" ).append( File.separator ).append( fileName );
        return path.toString();
	}

}