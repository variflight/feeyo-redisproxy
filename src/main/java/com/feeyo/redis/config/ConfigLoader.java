package com.feeyo.redis.config;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConfigLoader {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ConfigLoader.class );
	
	/**
	 * 获取 server.xml
	 * 
	 * @param uri
	 * @return
	 */
	public static Map<String, String> loadServerMap(String uri) {	
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
		}		
		return map;
		
	}
	
	/**
	 *  获取连接池配置
	 * 
	 * @param uri
	 * @return
	 */
	public static Map<Integer, PoolCfg> loadPoolMap(String uri) {

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
					poolCfg.addNode( ip + ":" + port );
				}
				map.put(id,  poolCfg);
			}
		} catch (Exception e) {
			LOGGER.error("loadPoolCfg err " + e);
		}
		return map;
	}
	
	public static Map<String, UserCfg> loadUserMap(Map<Integer, PoolCfg> poolMap, String uri) {

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
				
				boolean isReadonly = getBooleanAttribute(nameNodeMap, "readonly", false);
					
				PoolCfg poolCfg = poolMap.get(poolId);
				int poolType = poolCfg.getType();
				
				UserCfg userCfg = new UserCfg(poolId, poolType, password, prefix, selectDb, isAdmin == 0 ? false : true, isReadonly);
				map.put(password, userCfg);
			}
		} catch (Exception e) {
			LOGGER.error("loadUsers err " + e);
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
                            boolean usingZk = Boolean.valueOf(e.getAttribute("value"));
                            zkCfg.setUsingZk(usingZk);
                            break;
                        case "autoActivation":
                            boolean autoActivation = Boolean.valueOf(e.getAttribute("value"));
                            zkCfg.setAutoAct(autoActivation);
                            break;
                        case "zkHome":
                            zkCfg.setZkHome(e.getAttribute("value"));
                            break;
                        case "node":
                            zkCfg.getZkServers().add(e.getAttribute("ipAndPort"));
                            break;
                        case "conf":
                            zkCfg.getLocCfgNames().add(e.getAttribute("name"));
                            break;
                        default:
                            LOGGER.error("load zookeeper configure error", e);
                            return null;
                        }
	                }
	            }
	        }
	    } catch (Exception e) {
	        LOGGER.error("load zookeeper configure error", e);
        }
	    return zkCfg;
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
	
	private static boolean getBooleanAttribute(NamedNodeMap map, String attr, boolean defaultVal) {
		return getBooleanValue(map.getNamedItem(attr), defaultVal);
	}


	private static String getValue(Node node, String defaultVal) {
		return node == null ? defaultVal : node.getNodeValue();
	}

	private static int getIntValue(Node node, int defaultVal) {
		return node == null ? defaultVal : Integer.valueOf(node.getNodeValue());
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