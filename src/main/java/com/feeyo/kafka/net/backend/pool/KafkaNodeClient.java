package com.feeyo.kafka.net.backend.pool;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

public class KafkaNodeClient {
	
	private Node node = null;
	private Metrics metrics = null;
	private NetworkClient client = null;
	private Selector selector = null;
	private ChannelBuilder channelBuilder = null;
	
	public KafkaNodeClient(int id, String host, int port) {
		node = new Node(id, host, port);
		
		//
		LogContext logContext = new LogContext("ctx");

		ConfigDef defConf = new ConfigDef();
		defConf.define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, ConfigDef.Type.STRING,
				CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, ConfigDef.Importance.MEDIUM,
				CommonClientConfigs.SECURITY_PROTOCOL_DOC);

		defConf.define(SaslConfigs.SASL_MECHANISM, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_SASL_MECHANISM,
				ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_MECHANISM_DOC);

		metrics = new Metrics(Time.SYSTEM);

		AbstractConfig config = new AbstractConfig(defConf, new Properties());
		channelBuilder = ClientUtils.createChannelBuilder(config);
		selector = new Selector(1000L, metrics, Time.SYSTEM, "cc", channelBuilder, logContext);
		client = new NetworkClient(selector, new Metadata(0, Long.MAX_VALUE, false),
				Thread.currentThread().getName(), 10, 1000L, 1000L, 1, 1024, 1000, Time.SYSTEM, true, new ApiVersions(),
				null, logContext);
	}
	
	public ClientRequest newClientRequest(AbstractRequest.Builder<?> requestBuilder) {

		if (client == null || node == null)
			return null;
		
		ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, 1000L, true);
		return clientRequest;

	}

	public ClientResponse sendAndRecvice(ClientRequest clientRequest) throws IOException {
		
		//
		NetworkClientUtils.awaitReady(client, node, Time.SYSTEM, 5000);
		
		//
		ClientResponse response = NetworkClientUtils.sendAndReceive(client, clientRequest, Time.SYSTEM);
		return response;
	}
	
	
	
	public void close() {
		closeQuietly(metrics, "Metrics");
		closeQuietly(client, "NetworkClient");
		closeQuietly(selector, "Selector");
		closeQuietly(channelBuilder, "ChannelBuilder");
	}

}
