package com.feeyo.config.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;

public class ConfigLoaderFactory {
    private static Logger LOGGER = LoggerFactory.getLogger(ConfigLoaderFactory.class);

    public static AbstractConfigLoader getConfigLoader() {
        Map<String, String> clusterMap = loadCluster();
        if (clusterMap.get("id") == null || clusterMap.get("id").isEmpty()) {
            return new LocalConfigLoader();
        } else {
            return new RemoteConfigLoader(clusterMap.get("id"), clusterMap.get("zkServerHost"));
        }
    }

    private static Map<String, String> loadCluster() {
        String uri = AbstractConfigLoader.buidCfgAbsPathFor("cluster.xml");
        Map<String, String> map = new HashMap<>();
        try {
            Element element = AbstractConfigLoader.loadXmlDoc(uri).getDocumentElement();
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
            LOGGER.error("load server.xml err " + e);
        }
        return map;
    }
}
