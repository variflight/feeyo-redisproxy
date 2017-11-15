package com.feeyo.redis.engine.manage.stat;

import com.feeyo.redis.net.backend.pool.PhysicalNode;

public class TopHundredCollectMsg {
	
	private boolean isWrongType = false;
	private final PhysicalNode physicalNode;
	
	public TopHundredCollectMsg(boolean isWrongType, PhysicalNode physicalNode) {
		super();
		this.isWrongType = isWrongType;
		this.physicalNode = physicalNode;
	}

	public boolean isWrongType() {
		return isWrongType;
	}

	public PhysicalNode getPhysicalNode() {
		return physicalNode;
	}
	
}
