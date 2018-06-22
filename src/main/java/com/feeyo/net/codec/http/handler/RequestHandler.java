package com.feeyo.net.codec.http.handler;

import com.feeyo.net.nio.ClosableConnection;

public interface RequestHandler {

	void handle(ClosableConnection conn, String uri, byte[] data);

}

