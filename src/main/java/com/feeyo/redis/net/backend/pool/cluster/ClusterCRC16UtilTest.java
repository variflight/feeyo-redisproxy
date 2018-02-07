package com.feeyo.redis.net.backend.pool.cluster;

public class ClusterCRC16UtilTest {

	private static final int TOTAL_OPERATIONS = 100000000;

	private static String[] TEST_SET = { "", "123456789", "sfger132515", "hae9Napahngaikeethievubaibogiech",
			"AAAAAAAAAAAAAAAAAAAAAA", "Hello, World!" };

	public static void main(String[] args) {
		
		long begin = System.currentTimeMillis();
		for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
			ClusterCRC16Util.getSlot( (TEST_SET[n % TEST_SET.length]) );
		}

		long elapsed = System.currentTimeMillis() - begin;
		System.out.println(((1000 * TOTAL_OPERATIONS) / elapsed) + " ops");
	}

}
