package com.feeyo.kafka.util;

import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * BIO 实现，独立通道用于心跳及信息检测服务
 *
 * @author zhuam
 */
public class BIOConnection {
    // socket default info
    public static final int DEFAULT_TIMEOUT = 2000;

    private String host;
    private int port;
    private int connectionTimeout = DEFAULT_TIMEOUT;
    private int soTimeout = 0;

    private Socket socket;
    private OutputStream outputStream;
    private InputStream inputStream;

    public BIOConnection(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public BIOConnection(final String host, final int port, int connectionTimeout, int soTimeout) {
        this(host, port);
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
    }

    public void connect() {

        if (!isConnected()) {

            try {
                socket = new Socket();
                socket.setReuseAddress(true);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                socket.setSoLinger(true, 0);

                socket.connect(new InetSocketAddress(host, port), connectionTimeout);
                if (soTimeout > 0) socket.setSoTimeout(soTimeout);

                outputStream = socket.getOutputStream();
                inputStream = socket.getInputStream();

            } catch (IOException ex) {
                throw new KafkaException("Failed connecting to kafka host " + host + ":" + port, ex);
            }
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected() && !socket.isInputShutdown() &&
                !socket.isOutputShutdown();
    }

    public void send(final byte[] request) {
        try {
            connect();
            outputStream.write(request);

        } catch (IOException ex) {
            throw new KafkaException("Failed to send kafka server host " + host + ":" + port, ex);
        }
    }

    public byte[] receive() {
        byte[] response = new byte[1024];
        try {
            inputStream.read(response);
        } catch (IOException ex) {
            throw new KafkaException("Failed to receive kafka server host " + host + ":" + port, ex);
        }
        return response;
    }

    public void disconnect() {

        if (isConnected()) {
            try {
                outputStream.flush();
                socket.close();
            } catch (IOException ex) {
                throw new KafkaException("Failed to close connection with kafka host " + host + ":" + port, ex);
            } finally {

                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // ignored
                    }
                }
            }
        }
    }
}