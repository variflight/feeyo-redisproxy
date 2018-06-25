package com.feeyo.net.codec.http.test;

public interface RequestHandler {

	void handle(HttpConnection conn, String uri, byte[] data);

}
