package com.feeyo.kafka.net.backend.metadata;

public class DataNode {
	
	private final int id;
	private final String host;
	private final int port;
	
	public DataNode(int id, String host, int port) {
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
