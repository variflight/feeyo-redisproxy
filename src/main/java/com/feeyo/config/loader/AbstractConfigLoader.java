package com.feeyo.config.loader;

import com.feeyo.config.NetFlowCfg;
import com.feeyo.config.PoolCfg;
import com.feeyo.config.UserCfg;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class AbstractConfigLoader {
    /**
     * 获取 server 配置
     */
    public abstract Map<String, String> loadServerMap() throws Exception;

    /**
     * 获取连接池配置
     */
    public abstract Map<Integer, PoolCfg> loadPoolMap() throws Exception;

    /**
     * 获取user配置
     */
    public abstract Map<String, UserCfg> loadUserMap(Map<Integer, PoolCfg> poolMap) throws Exception;

    /**
     * 获取流量控制配置
     */
    public abstract Map<String, NetFlowCfg> loadNetFlowMap() throws Exception;

    /**
     * 邮件服务配置
     */
    public abstract Properties loadMailProperties() throws Exception;

    public static Document loadXmlDoc(String uri) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(uri);
        return doc;
    }

    protected String getAttribute(NamedNodeMap map, String attr, String defaultVal) {
        return getValue(map.getNamedItem(attr), defaultVal);
    }

    protected float getFloatAttribute(NamedNodeMap map, String attr, float defaultVal) {
        return getFloatValue(map.getNamedItem(attr), defaultVal);
    }

    protected int getIntAttribute(NamedNodeMap map, String attr, int defaultVal) {
        return getIntValue(map.getNamedItem(attr), defaultVal);
    }

    protected short getShortAttribute(NamedNodeMap map, String attr, short defaultVal) {
        return getShortValue(map.getNamedItem(attr), defaultVal);
    }

    protected boolean getBooleanAttribute(NamedNodeMap map, String attr, boolean defaultVal) {
        return getBooleanValue(map.getNamedItem(attr), defaultVal);
    }


    private String getValue(Node node, String defaultVal) {
        return node == null ? defaultVal : node.getNodeValue();
    }

    private float getFloatValue(Node node, float defaultVal) {
        return node == null ? defaultVal : Float.valueOf(node.getNodeValue());
    }

    private int getIntValue(Node node, int defaultVal) {
        return node == null ? defaultVal : Integer.valueOf(node.getNodeValue());
    }

    private short getShortValue(Node node, short defaultVal) {
        return node == null ? defaultVal : Short.valueOf(node.getNodeValue());
    }

    private boolean getBooleanValue(Node node, boolean defaultVal) {
        return node == null ? defaultVal : Boolean.valueOf(node.getNodeValue());
    }

    protected List<Node> getChildNodes(Node theNode, String childElName) {
        LinkedList<Node> nodes = new LinkedList<Node>();
        NodeList childs = theNode.getChildNodes();
        for (int j = 0; j < childs.getLength(); j++) {
            if (childs.item(j).getNodeType() == Document.ELEMENT_NODE && childs.item(j).getNodeName().equals(childElName)) {
                nodes.add(childs.item(j));
            }
        }
        return nodes;
    }

    protected Pattern loadDefaultKeyRule(String uri) {

        try {
            NodeList nodeList = loadXmlDoc(uri).getElementsByTagName("all");
            if (nodeList != null && nodeList.getLength() > 0) {
                Node node = nodeList.item(0);
                NamedNodeMap nameNodeMap = node.getAttributes();
                String rule = getAttribute(nameNodeMap, "defaultKeyRule", null);
                if (rule != null  && !rule.isEmpty()) {
                    return Pattern.compile(rule);
                }
            }
        } catch (Exception e) {
        }
        return null;
    }

    public static String buidCfgAbsPathFor(String fileName) {
        StringBuffer path = new StringBuffer();
        path.append(System.getProperty("FEEYO_HOME")).append(File.separator);
        path.append("conf").append(File.separator).append(fileName);
        return path.toString();
    }

}
