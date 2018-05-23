package com.feeyo.kafka.net.backend.broker;

public class BrokerNode {
	
	private final int id;
	private final String host;
	private final int port;
	
	public BrokerNode(int id, String host, int port) {
		this.id = id;
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getId() {
		return id;
	}

	public int getPort() {
		return port;
	}
	
}
