package com.feeyo.redis.net.backend;

import java.io.IOException;

import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;

public interface BackendConnectionFactory {

	public BackendConnection make(PhysicalNode physicalNode, 
			BackendCallback callback, Object attachement) throws IOException;
	
}
