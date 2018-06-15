package com.feeyo.net.codec.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.ByteUtil;
import com.google.protobuf.MessageLite;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class PBHttpServerHandler implements HttpHandler {

	private final Logger LOGGER = LoggerFactory.getLogger(PBHttpServerHandler.class);
	@Override
	public void handle(HttpExchange httpExchange) throws IOException {

		List<String> messageTypes = httpExchange.getRequestHeaders().get("message-type");
		if (messageTypes == null || messageTypes.size() != 1) {
			LOGGER.error("message-type empty or over limit 1");
			return;
		}
		String messageType = messageTypes.get(0);

		Class<?> clazz;
		OutputStreamWriter writer = null;
		OutputStream out = null;
		try {
			clazz = Class.forName(messageType);

			Method method = clazz.getMethod("getDefaultInstance");
			MessageLite proto = (MessageLite) method.invoke(clazz);
			httpExchange.getHttpContext();
			InputStream in = httpExchange.getRequestBody(); // 获得输入流
			byte[] buf = ByteUtil.inputStream2byte(in);

			PBDecoder decoder = new PBDecoder(proto, false);
			List<MessageLite> msgList = decoder.decode(buf);
			System.out.println("receive msg number -> " + msgList.size());

			String response = "receive msg successful";
			httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.getBytes("UTF-8").length);
			out = httpExchange.getResponseBody();
			writer = new OutputStreamWriter(out, "UTF-8");
			writer.write(response);
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				writer.close();
			}
			if (out != null) {
				out.close();
			}
			httpExchange.close();
		}
	}

}
