package com.feeyo.redis.engine.manage.node;


public class ClusterInfo {
	
	enum ClusterState {
		OK,              // 一切正常
		SLOTS_MISSING, 
		SLOTS_FAIL, 
		SLOTS_PFAIL
	}
	
	private ClusterState clusterState;
	private int clusterSlotsCount;
	private int clusterOkSlotsCount;
	private int clusterFailSlotsCount;
	private int clusterPFailSlotsCount;

	public ClusterInfo(ClusterState clusterState, int clusterSlotsCount, int clusterOkSlotsCount,
			int clusterFailSlotsCount, int clusterPFailSlotsCount) {
		this.clusterState = clusterState;
		this.clusterSlotsCount = clusterSlotsCount;
		this.clusterOkSlotsCount = clusterOkSlotsCount;
		this.clusterFailSlotsCount = clusterFailSlotsCount;
		this.clusterPFailSlotsCount = clusterPFailSlotsCount;
	}

	public ClusterState getClusterState() {
		return clusterState;
	}

	public int getClusterSlotsCount() {
		return clusterSlotsCount;
	}

	public int getClusterOkSlotsCount() {
		return clusterOkSlotsCount;
	}

	public int getClusterFailSlotsCount() {
		return clusterFailSlotsCount;
	}

	public int getClusterPFailSlotsCount() {
		return clusterPFailSlotsCount;
	}
}


