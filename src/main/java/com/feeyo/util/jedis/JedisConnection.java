package com.feeyo.util.jedis;

import com.feeyo.util.jedis.exception.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * BIO 实现，独立通道用于心跳及信息检测服务
 * 
 * @author zhuam
 *
 */
public class JedisConnection {
	
	// redis command header
	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';
	public static final byte MINUS_BYTE = '-';
	public static final byte COLON_BYTE = ':';
	
	// socket default info
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 6379;
	public static final int DEFAULT_TIMEOUT = 2000;
	public static final int DEFAULT_DATABASE = 0;
	public static final String CHARSET = "UTF-8";

	private static final byte[][] EMPTY_ARGS = new byte[0][];

	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private int connectionTimeout = DEFAULT_TIMEOUT;
	private int soTimeout = 0;
	private boolean broken = false;

	private Socket socket;
	private RedisOutputStream outputStream;
	private RedisInputStream inputStream;
	
	protected Pool<JedisConnection> dataSource = null;	

	public JedisConnection(final String host, final int port) {
		this.host = host;
		this.port = port;
	}
	
	public JedisConnection(final String host, final int port, int connectionTimeout, int soTimeout ) {
		this.host = host;
		this.port = port;
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
				if ( soTimeout > 0)
		          socket.setSoTimeout(soTimeout);
		        
		        outputStream = new RedisOutputStream(socket.getOutputStream());
		        inputStream = new RedisInputStream(socket.getInputStream());
		        
			} catch (IOException ex) {
				broken = true;
				throw new JedisConnectionException("Failed connecting to host " + host + ":" + port, ex);
			}
		}
	}

	public void disconnect() {
        try {
            if ( outputStream != null)
                outputStream.flush();

            if ( socket != null ) {
                socket.close();
                socket = null;
            }

        } catch (IOException ex) {
            broken = true;
            throw new JedisConnectionException(ex);
        } finally {

            if ( inputStream != null )
                try {
                    inputStream.close();
                    inputStream = null;
                } catch (IOException e1) {
                    // ignored
                }

            if ( outputStream != null)
                try {
                    outputStream.close();
                    outputStream = null;
                } catch (IOException e1) {
                    // ignore
                }

            if (socket != null) {
                try {
                    socket.close();
                    socket = null;
                } catch (IOException e) {
                    // ignored
                }
            }
        }
	}
	
	public void close() {
		if (dataSource != null) {
			if (isBroken()) {
				this.dataSource.returnBrokenResource(this);
			} else {
				this.dataSource.returnResource(this);
			}
		} else {
			disconnect();
		}
	}

	public void setDataSource(Pool<JedisConnection> jedisPool) {
		this.dataSource = jedisPool;
	}
	
	public boolean isConnected() {
		return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
				&& !socket.isInputShutdown() && !socket.isOutputShutdown();
	}
	
	public boolean isBroken() {
		return broken;
	}
	
	// write  get reply
	//-----------------------------------------------------------------
	protected void flush() {
		try {
			outputStream.flush();
		} catch (IOException ex) {
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}
	
	public String getStatusCodeReply() {
		flush();
		final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
		if (null == resp) {
			return null;
		} else {
			return SafeEncoder.encode(resp);
		}
	}

	public String getBulkReply() {
		final byte[] result = getBinaryBulkReply();
		if (null != result) {
			return SafeEncoder.encode(result);
		} else {
			return null;
		}
	}

	public byte[] getBinaryBulkReply() {
		flush();
		return (byte[]) readProtocolWithCheckingBroken();
	}

	public Long getIntegerReply() {
		flush();
		return (Long) readProtocolWithCheckingBroken();
	}

	public List<String> getMultiBulkReply() {

		List<byte[]> data = getBinaryMultiBulkReply();

		if (null == data) {
			return Collections.emptyList();
		}
		List<byte[]> l = (List<byte[]>) data;
		
		final ArrayList<String> result = new ArrayList<String>(l.size());
		for (final byte[] barray : l) {
			if (barray == null) {
				result.add(null);
			} else {
				result.add(SafeEncoder.encode(barray));
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public List<byte[]> getBinaryMultiBulkReply() {
		flush();
		return (List<byte[]>) readProtocolWithCheckingBroken();
	}

	@SuppressWarnings("unchecked")
	public List<Object> getRawObjectMultiBulkReply() {
		return (List<Object>) readProtocolWithCheckingBroken();
	}

	public List<Object> getObjectMultiBulkReply() {
		flush();
		return getRawObjectMultiBulkReply();
	}

	@SuppressWarnings("unchecked")
	public List<Long> getIntegerMultiBulkReply() {
		flush();
		return (List<Long>) readProtocolWithCheckingBroken();
	}
	
	public List<Object> getMany(final int count) {
		flush();
		final List<Object> responses = new ArrayList<Object>(count);
		for (int i = 0; i < count; i++) {
			try {
				responses.add(readProtocolWithCheckingBroken());
			} catch (JedisDataException e) {
				responses.add(e);
			}
		}
		return responses;
	}

	public Object getOne() {
		flush();
		return readProtocolWithCheckingBroken();
	}

	protected Object readProtocolWithCheckingBroken() {
		try {
			return read(inputStream);
		} catch (JedisConnectionException exc) {
			broken = true;
			throw exc;
		}
	}	
	
	
	
	// write command but not flush
	// ---------------------------------------------------------------------------	
	public void sendCommand(final RedisCommand cmd, final String... args) {
		final byte[][] bargs = new byte[args.length][];
		for (int i = 0; i < args.length; i++) {
			bargs[i] = SafeEncoder.encode(args[i]);
		}
		sendCommand(cmd, bargs);
	}
	
	public void sendCommand(final RedisCommand cmd) {
	    sendCommand(cmd, EMPTY_ARGS);
	}
	
	public void sendCommand(final byte[]... args) {
		try {
			connect();
			sendCommand(outputStream, args);
			
		} catch (JedisConnectionException ex) {
			/*
			 * When client send request which formed by invalid protocol, Redis
			 * send back error message before close connection. We try to read
			 * it to provide reason of failure.
			 */
			try {
				String errorMessage = readErrorLineIfPossible(inputStream);
				if (errorMessage != null && errorMessage.length() > 0) {
					ex = new JedisConnectionException(errorMessage, ex.getCause());
				}
			} catch (Exception e) {
				/*
				 * Catch any IOException or JedisConnectionException occurred
				 * from InputStream#read and just ignore. This approach is safe
				 * because reading error message is optional and connection will
				 * eventually be closed.
				 */
			}
			// Any other exceptions related to connection?
			broken = true;
			throw ex;
		}
	}

	public void sendCommand(final RedisCommand cmd, final byte[]... args) {
		try {
			connect();
			sendCommand(outputStream, cmd.getRaw(), args);
			
		} catch (JedisConnectionException ex) {
			/*
			 * When client send request which formed by invalid protocol, Redis
			 * send back error message before close connection. We try to read
			 * it to provide reason of failure.
			 */
			try {
				String errorMessage = readErrorLineIfPossible(inputStream);
				if (errorMessage != null && errorMessage.length() > 0) {
					ex = new JedisConnectionException(errorMessage, ex.getCause());
				}
			} catch (Exception e) {
				/*
				 * Catch any IOException or JedisConnectionException occurred
				 * from InputStream#read and just ignore. This approach is safe
				 * because reading error message is optional and connection will
				 * eventually be closed.
				 */
			}
			// Any other exceptions related to connection?
			broken = true;
			throw ex;
		}
		
	}
	
	private void sendCommand(final RedisOutputStream os, final byte[] command, final byte[]... args) {
		try {
			os.write(ASTERISK_BYTE);
			os.writeIntCrLf(args.length + 1);
			os.write(DOLLAR_BYTE);
			os.writeIntCrLf(command.length);
			os.write(command);
			os.writeCrLf();

			for (final byte[] arg : args) {
				os.write(DOLLAR_BYTE);
				os.writeIntCrLf(arg.length);
				os.write(arg);
				os.writeCrLf();
			}
		} catch (IOException e) {
			throw new JedisConnectionException(e);
		}
	}
	
	private void sendCommand(final RedisOutputStream os, final byte[]... args) {
		try {
			os.write(ASTERISK_BYTE);
			os.writeIntCrLf(args.length);
			
			for (final byte[] arg : args) {
				os.write(DOLLAR_BYTE);
				os.writeIntCrLf(arg.length);
				os.write(arg);
				os.writeCrLf();
			}
		} catch (IOException e) {
			throw new JedisConnectionException(e);
		}
	}
	
	
	// read reply and parse
	// -------------------------------------------------------------------
	public static Object read(final RedisInputStream is) {
		return process(is);
	}
	
	public static String readErrorLineIfPossible(RedisInputStream is) {
		final byte b = is.readByte();
		// if buffer contains other type of response, just ignore.
		if (b != MINUS_BYTE) {
			return null;
		}
		return is.readLine();
	}
	
	private static Object process(final RedisInputStream is) {

		final byte b = is.readByte();
		if (b == PLUS_BYTE) {
			return processStatusCodeReply(is);
		} else if (b == DOLLAR_BYTE) {
			return processBulkReply(is);
		} else if (b == ASTERISK_BYTE) {
			return processMultiBulkReply(is);
		} else if (b == COLON_BYTE) {
			return processInteger(is);
		} else if (b == MINUS_BYTE) {
			processError(is);
			return null;
		} else {
			throw new JedisConnectionException("Unknown reply: " + (char) b);
		}
	}
	
	private static byte[] getBinaryReply(final RedisInputStream is) {
		try {
			return is.readAll();
		} catch (Exception e) {
			throw new JedisConnectionException(e);
		}
	}
	
	public byte[] getBinaryReply() {
		flush();
		return getBinaryReply(inputStream);
	}
	
	private static byte[] processStatusCodeReply(final RedisInputStream is) {
		return is.readLineBytes();
	}

	private static byte[] processBulkReply(final RedisInputStream is) {
		final int len = is.readIntCrLf();
		if (len == -1) {
			return null;
		}

		final byte[] read = new byte[len];
		int offset = 0;
		while (offset < len) {
			final int size = is.read(read, offset, (len - offset));
			if (size == -1)
				throw new JedisConnectionException("It seems like server has closed the connection.");
			offset += size;
		}

		// read 2 more bytes for the command delimiter
		is.readByte();
		is.readByte();

		return read;
	}
	
	private static Long processInteger(final RedisInputStream is) {
		return is.readLongCrLf();
	}

	private static List<Object> processMultiBulkReply(final RedisInputStream is) {
		final int num = is.readIntCrLf();
		if (num == -1) {
			return null;
		}
		final List<Object> ret = new ArrayList<Object>(num);
		for (int i = 0; i < num; i++) {
			try {
				ret.add(process(is));
			} catch (JedisDataException e) {
				ret.add(e);
			}
		}
		return ret;
	}
	
	// process info
	// -------------------------------------------------------------------------------
	private static final String ASK_RESPONSE = "ASK";
	private static final String MOVED_RESPONSE = "MOVED";
	private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
	private static final String BUSY_RESPONSE = "BUSY";
	private static final String NOSCRIPT_RESPONSE = "NOSCRIPT";
	
	private static void processError(final RedisInputStream is) {
		String message = is.readLine();
		// TODO: I'm not sure if this is the best way to do this.
		// Maybe Read only first 5 bytes instead?
		if (message.startsWith(MOVED_RESPONSE)) {
			String[] movedInfo = parseTargetHostAndSlot(message);
			throw new JedisMovedDataException(message, new HostAndPort(movedInfo[1], Integer.valueOf(movedInfo[2])),
					Integer.valueOf(movedInfo[0]));
		} else if (message.startsWith(ASK_RESPONSE)) {
			String[] askInfo = parseTargetHostAndSlot(message);
			throw new JedisAskDataException(message, new HostAndPort(askInfo[1], Integer.valueOf(askInfo[2])),
					Integer.valueOf(askInfo[0]));
		} else if (message.startsWith(CLUSTERDOWN_RESPONSE)) {
			throw new JedisClusterException(message);
		} else if (message.startsWith(BUSY_RESPONSE)) {
			throw new JedisBusyException(message);
		} else if (message.startsWith(NOSCRIPT_RESPONSE)) {
			throw new JedisNoScriptException(message);
		}
		throw new JedisDataException(message);
	}
	
	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	public String ping() {
		sendCommand(RedisCommand.PING);
		return getStatusCodeReply();
	}
	
	private static String[] parseTargetHostAndSlot(String clusterRedirectResponse) {
		String[] response = new String[3];
		String[] messageInfo = clusterRedirectResponse.split(" ");
		String[] targetHostAndPort = HostAndPort.extractParts(messageInfo[2]);
		response[0] = messageInfo[1];
		response[1] = targetHostAndPort[0];
		response[2] = targetHostAndPort[1];
		return response;
	}	
}