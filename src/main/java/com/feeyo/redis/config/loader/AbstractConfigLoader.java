package com.feeyo.redis.config.loader;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class AbstractConfigLoader {

	protected static Document loadXmlDoc(String uri) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(uri);
		return doc;
	}

	protected static String getAttribute(NamedNodeMap map, String attr, String defaultVal) {
		return getValue(map.getNamedItem(attr), defaultVal);
	}

	protected static float getFloatAttribute(NamedNodeMap map, String attr, float defaultVal) {
		return getFloatValue(map.getNamedItem(attr), defaultVal);
	}

	protected static int getIntAttribute(NamedNodeMap map, String attr, int defaultVal) {
		return getIntValue(map.getNamedItem(attr), defaultVal);
	}

	static short getShortAttribute(NamedNodeMap map, String attr, short defaultVal) {
		return getShortValue(map.getNamedItem(attr), defaultVal);
	}

	protected static boolean getBooleanAttribute(NamedNodeMap map, String attr, boolean defaultVal) {
		return getBooleanValue(map.getNamedItem(attr), defaultVal);
	}

	private static String getValue(Node node, String defaultVal) {
		return node == null ? defaultVal : node.getNodeValue();
	}

	private static float getFloatValue(Node node, float defaultVal) {
		return node == null ? defaultVal : Float.valueOf(node.getNodeValue());
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

	protected static List<Node> getChildNodes(Node theNode, String childElName) {
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

	public AbstractConfigLoader() {
		super();
	}

}