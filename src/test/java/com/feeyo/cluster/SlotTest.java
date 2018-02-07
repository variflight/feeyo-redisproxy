package com.feeyo.cluster;
import static org.junit.Assert.*;

import org.junit.Test;

import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;


public class SlotTest {

	@Test
	public void test() {
		assertEquals( ClusterCRC16Util.getSlot("key1"), 9189 );
	}

}
