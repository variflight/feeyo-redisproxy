package com.feeyo.redis.nio;

import java.io.IOException;

/**
 * rector 线程池 
 * 
 * @author zhuam
 *
 */
public class NIOReactorPool {

	private final NIOReactor[] reactors;
	private volatile int nextReactor;	// 上一次处理连接的reactor，使用volatile保证多线程操作时内存可见

	public NIOReactorPool(String name, int poolSize) throws IOException {
		reactors = new NIOReactor[poolSize];
		for (int i = 0; i < poolSize; i++) {
			NIOReactor reactor = new NIOReactor(name + "-" + i);
			reactors[i] = reactor;
			reactor.startup();
		}
	}
	
	public NIOReactor[] getAllReactors() {
		return reactors;
	}

	/**
	 * 获取下一个处理连接的reactor, 轮询策略
	 * @return
	 */
	public NIOReactor getNextReactor() {
	 	int i = ++nextReactor;
		if (i >= reactors.length) {
			i = nextReactor = 0;
		}
		return reactors[i];
	}
}
