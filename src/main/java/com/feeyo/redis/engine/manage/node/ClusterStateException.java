package com.feeyo.redis.engine.manage.node;


public class ClusterStateException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private ClusterInfo clusterInfo;

	public ClusterStateException(String message, ClusterInfo clusterInfo) {
		super(message);
		this.clusterInfo = clusterInfo;
	}

	public ClusterStateException(String message, Throwable cause, ClusterInfo clusterInfo) {
		super(message, cause);
		this.clusterInfo = clusterInfo;
	}

	public ClusterInfo getClusterInfo() {
		return clusterInfo;
	}
}
