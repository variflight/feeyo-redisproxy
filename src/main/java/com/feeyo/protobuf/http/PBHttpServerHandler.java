package com.feeyo.protobuf.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.util.List;

import com.feeyo.util.ByteUtil;
import com.google.protobuf.MessageLiteOrBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class PBHttpServerHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {

		InputStream in = httpExchange.getRequestBody(); // 获得输入流
		
		byte[] buf = ByteUtil.inputStream2byte(in);
		
		MessageWrapper wrapper = new MessageWrapper();
		List<MessageLiteOrBuilder> msgList = wrapper.wrapOut(buf);
		
		System.out.println("receive msg number -> " + msgList.size() );
		
		String response = "receive msg successful";
		httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.getBytes("UTF-8").length);
		OutputStream responseBody = httpExchange.getResponseBody();
		OutputStreamWriter writer = new OutputStreamWriter(responseBody, "UTF-8");
		writer.write(response);
		writer.close();
		responseBody.close();
		httpExchange.close();
        
	}
	
}
