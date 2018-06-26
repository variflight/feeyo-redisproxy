package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.offset.BrokerOffsetService;
import com.feeyo.kafka.net.front.handler.KafkaCommandHandler;
import com.feeyo.net.codec.UnknowProtocolException;
import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisRequestDecoderV2;
import com.feeyo.net.codec.redis.RedisRequestPolicy;
import com.feeyo.net.codec.redis.RedisRequestType;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.Manage;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.front.handler.AbstractCommandHandler;
import com.feeyo.redis.net.front.handler.BlockCommandHandler;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.handler.DefaultCommandHandler;
import com.feeyo.redis.net.front.handler.PipelineCommandHandler;
import com.feeyo.redis.net.front.handler.PubSub;
import com.feeyo.redis.net.front.handler.segment.SegmentCommandHandler;
import com.feeyo.redis.net.front.route.FullRequestNoThroughtException;
import com.feeyo.redis.net.front.route.InvalidRequestException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteService;
import com.feeyo.util.ProtoUtils;

public class RedisFrontSession {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisFrontSession.class );

	public static final Charset _charset = Charset.forName( "UTF-8" );
	public static final byte[] OK =   "+OK\r\n".getBytes();
	public static final byte[] PONG =  "+PONG\r\n".getBytes();
	public static final byte[] ERR_NO_AUTH = "-NOAUTH Authentication required.\r\n".getBytes();	
	public static final byte[] ERR_NO_AUTH_PASSWORD = "-ERR invalid password.\r\n".getBytes();
	public static final byte[] ERR_NO_AUTH_NO_PASSWORD = "-ERR Client sent AUTH, but no password is set\r\n".getBytes();
	public static final byte[] ERR_PIPELINE_BACKEND = "-ERR pipeline error\r\n".getBytes();
	public static final byte[] ERR_INVALID_COMMAND = "-ERR invalid command exist.\r\n".getBytes();
	
	public static final String NOT_SUPPORTED = "Not supported.";
	public static final String NOT_ADMIN_USER = "Not supported:manage cmd but not admin user.";
	public static final String UNKNOW_COMMAND = "Unknow command.";
	public static final String NOT_READ_COMMAND = "Not read command.";

	public static final byte[] NULL =  "$-1\r\n".getBytes();
	
	// PUBSUB
	private PubSub pubsub = null;
	
	// 
	private String requestCmd;
	private String requestKey;
	private int requestSize;
	private long requestTimeMills; 
	
    // 解析器 
	private RedisRequestDecoderV2 requestDecoder = new RedisRequestDecoderV2();
	
	private AbstractCommandHandler defaultCommandHandler;
	private AbstractCommandHandler segmentCommandHandler;
	private AbstractCommandHandler pipelineCommandHandler;
	private AbstractCommandHandler blockCommandHandler;
	private AbstractCommandHandler kafkaCommandHandler;
	
	private AbstractCommandHandler currentCommandHandler;
	
	private Object _lock = new Object();
	
	private RedisFrontConnection frontCon;

	public RedisFrontSession(RedisFrontConnection frontCon) {
		this.frontCon = frontCon;
	}

	public void handle(byte[] byteBuff) {
		
		// 默认需要立即释放
		boolean isImmediateReleaseConReadLock = true;
		
		List<RedisRequest> requests = null;
		RedisRequest firstRequest = null;
		
		try {
			// parse
			requests = requestDecoder.decode(byteBuff);
			if (requests == null || requests.size() == 0 ) {
				return;
			}
			
			firstRequest = requests.get(0);
			
			// 非pipeline 情况下， 特殊指令前置优化性能
			if ( requests.size() ==  1 ) {
				
				byte[] cmd = firstRequest.getArgs()[0];
				int len = cmd.length;
				if ( len == 4 ) {
					
					// AUTH
					if ( (cmd[0] == 'A' || cmd[0] == 'a') && (cmd[1] == 'U' || cmd[1] == 'u') 
							&& (cmd[2] == 'T' || cmd[2] == 't') && (cmd[3] == 'H' || cmd[3] == 'h')   ) {
						
						if( firstRequest.getArgs().length < 2 ) {
							frontCon.write( ERR_NO_AUTH_NO_PASSWORD );
							return;
						}
						
						auth( firstRequest );
						return;
					
					// ECHO
					} else if ( (cmd[0] == 'E' || cmd[0] == 'e') && (cmd[1] == 'C' || cmd[1] == 'c') 
							 && (cmd[2] == 'H' || cmd[2] == 'h') && (cmd[3] == 'O' || cmd[3] == 'o') ) {
						echo( firstRequest );
						return;

					// PING
					} else if ( (cmd[0] == 'P' || cmd[0] == 'p') && (cmd[1] == 'I' || cmd[1] == 'i') 
							 && (cmd[2] == 'N' || cmd[2] == 'n') && (cmd[3] == 'G' || cmd[3] == 'g') ) {
						frontCon.write(PONG);
						return;
						
					// QUIT
					} else if ( (cmd[0] == 'Q' || cmd[0] == 'q') && (cmd[1] == 'U' || cmd[1] == 'u') 
							 && (cmd[2] == 'I' || cmd[2] == 'i') && (cmd[3] == 'T' || cmd[3] == 't') ) {
						frontCon.write(OK);
						frontCon.close("quit");
						return;
					}
					
				} else if ( len == 6 ) {
					// SELECT
					if ( (cmd[0] == 'S' || cmd[0] == 's') && (cmd[1] == 'E' || cmd[1] == 'e') 
							 && (cmd[2] == 'L' || cmd[2] == 'l') && (cmd[3] == 'E' || cmd[3] == 'e')
							 && (cmd[4] == 'C' || cmd[4] == 'c') && (cmd[5] == 'T' || cmd[5] == 't')) {
						frontCon.write(OK);
						return;
					}
				}
				
			} 
			
			// 认证
			if ( !frontCon.isAuthenticated() ) {
				
				byte[] cmd = firstRequest.getArgs()[0];
				if (cmd.length == 4 && 
						(cmd[0] == 'A' || cmd[0] == 'a') && 
						(cmd[1] == 'U' || cmd[1] == 'u') && 
						(cmd[2] == 'T' || cmd[2] == 't') && 
						(cmd[3] == 'H' || cmd[3] == 'h')) {

					boolean isPass = auth( firstRequest );
					if ( isPass ) {
						
						requests.remove(0);
						if ( requests.isEmpty() ) {
							return;
						}
						
					} else {
						return;
					}

				} else {
					frontCon.write(ERR_NO_AUTH);
					return;
				}	
			} 
			
			// 执行路由
			try {
				
				// 管理指令检测
				if ( frontCon.getUserCfg().isAdmin() && requests.size() == 1 ) {
					
					String cmd = new String( firstRequest.getArgs()[0] ).toUpperCase();
					RedisRequestPolicy policy = CommandParse.getPolicy( cmd );
					
					if( policy.getCategory() == CommandParse.MANAGE_CMD ) {
						byte[] buff = Manage.execute(firstRequest, frontCon);
						if (buff != null)
							frontCon.write(buff);
						return;
					}
				}
				
				// 指令路由
				RouteResult routeResult = RouteService.route(requests, frontCon);
				if ( routeResult == null ) {
					frontCon.write( "-ERR unkonw command \r\n".getBytes() );
					return;
				} 
				
				// 指令提前返回
				if ( intercept( routeResult ) ) {
					return;
				}
				
				currentCommandHandler = this.getCommandHandler( routeResult.getRequestType() );
				currentCommandHandler.handle(routeResult);
				
				if ( routeResult.getRequestType() != RedisRequestType.DEFAULT ) {
					// pipeline mget mset mdel 暂时不释放锁
					isImmediateReleaseConReadLock = false;
				}
				
			} catch (InvalidRequestException e) {
				
				if ( e.isIsfaultTolerant() ) {
				
					if ( requests.size() > 1 ) {
						frontCon.write( ERR_INVALID_COMMAND );
					} else {
						// 此处用于兼容
						frontCon.write( OK );
					}
					
				} else {
					
					// err
					StringBuffer errCmdBuffer = new StringBuffer(50);
					errCmdBuffer.append("-ERR ");
					errCmdBuffer.append( e.getMessage() );
					errCmdBuffer.append( ".\r\n" );
					
					byte[] ERR_INVALID_COMMAND = errCmdBuffer.toString().getBytes();
					frontCon.write( ERR_INVALID_COMMAND );
				}
				
				LOGGER.warn("con: {}, request err: {}", this.frontCon, requests);
			
			// auto response
			} catch (FullRequestNoThroughtException e) {

				for (int i = 0; i < e.getRequests().size(); i++) {
					RedisRequest request = e.getRequests().get(i);
					if (request == null) {
						continue;
					}
					String cmd = new String(request.getArgs()[0]).toUpperCase();
					if ("AUTH".equals(cmd)) {
						auth(request);
					} else if ("ECHO".equals(cmd)) {
						echo(request);
					} else if ("SELECT".equals(cmd)) {
						select(request);
					} else if ("PING".equals(cmd)) {
						frontCon.write(PONG);
					} else if ("QUIT".equals(cmd)) {
						frontCon.write(OK);
						frontCon.close("quit");
					}
				}
				
			// node unavailable
			} catch (PhysicalNodeUnavailableException e) {
				//-ERR node unavaliable error \r\n
				frontCon.write( "-ERR node unavailable error \r\n".getBytes() );
			}
			
		} catch (UnknowProtocolException e0) {
			frontCon.close("unknow redis client .");

		} catch (IOException e1) {
			String error = "-ERR " + e1.getMessage() + ".\r\n";
			frontCon.write(error.getBytes());
			
		} finally {
			
			if ( isImmediateReleaseConReadLock )
				frontCon.releaseLock();
		}
		
	}
	
	/**
	 * 获取CommandHandler
	 * @return
	 */
	private AbstractCommandHandler getCommandHandler(RedisRequestType redisRequestType) {
		switch (redisRequestType) {
		case DEFAULT:
			if (defaultCommandHandler == null) {
				synchronized (_lock) {
					if (defaultCommandHandler == null) {
						defaultCommandHandler = new DefaultCommandHandler( frontCon );
					}
				}
			}
			return defaultCommandHandler;
			
		case DEL_MULTIKEY:
		case MGET:
		case MSET:
		case MEXISTS:
			if (segmentCommandHandler == null) {
				synchronized (_lock) {
					if (segmentCommandHandler == null) {
						segmentCommandHandler = new SegmentCommandHandler( frontCon );
					}
				}
			}
			return segmentCommandHandler;
			
		case PIPELINE:
			if (pipelineCommandHandler == null) {
				synchronized (_lock) {
					if (pipelineCommandHandler == null) {
						pipelineCommandHandler = new PipelineCommandHandler( frontCon );
					}
				}
			}
			return pipelineCommandHandler;

		case BLOCK: 
			
			if (blockCommandHandler == null) {
				synchronized (_lock) {
					if (blockCommandHandler == null) {
						blockCommandHandler = new BlockCommandHandler( frontCon );
					}
				}
			}
			return blockCommandHandler;
			
		case KAFKA: 
			
			if (kafkaCommandHandler == null) {
				synchronized (_lock) {
					if (kafkaCommandHandler == null) {
						kafkaCommandHandler = new KafkaCommandHandler(frontCon);
					}
				}
			}
			return kafkaCommandHandler;
			
		default:
			return defaultCommandHandler;
		}
	}
	
	// Auth
	private boolean auth(RedisRequest request) {

		if (request.getArgs().length < 2) {
			frontCon.write(ERR_NO_AUTH_NO_PASSWORD);
			return false;
		}

		String password = new String(request.getArgs()[1], _charset);
		UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(password);
		if (userCfg != null) {
			frontCon.setUserCfg(userCfg);
			frontCon.setPassword(password);
			frontCon.setAuthenticated(true);
			frontCon.write(OK);
			return true;
		} else {
			frontCon.write(ERR_NO_AUTH_PASSWORD);
			return false;
		}
	}
	
	// Echo
	protected void echo(RedisRequest request) {	
		if ( request.getArgs().length != 2 ) {
			StringBuffer error = new StringBuffer();
			error.append("-");
			error.append("ERR wrong number of arguments for 'echo' command");
			frontCon.write( error.toString().getBytes() );
			
		} else {	
			byte[] arg1 = request.getArgs()[1];						
			byte[] dest = new byte[ 3 + arg1.length ];
			dest[0] = '+';
			dest[dest.length - 2] = '\r';
			dest[dest.length - 1] = '\n';
			System.arraycopy(arg1, 0, dest, 1, arg1.length );
			frontCon.write( dest );						
		}
	}
	
	// Select
	protected void select(RedisRequest request) {	
		if ( request.getArgs().length != 2 ) {
			StringBuffer error = new StringBuffer();
			error.append("-");
			error.append("ERR wrong number of arguments for 'echo' command");
			frontCon.write( error.toString().getBytes() );
			
		} else {	
			frontCon.write(OK);				
		}
	}
	
	public PubSub getPubsub() {
		return pubsub;
	}

	public void setPubsub(PubSub pubsub) {
		this.pubsub = pubsub;
	}

	// request info
	// ----------------------------------------------
	public String getRequestCmd() {
		return requestCmd;
	}
	
	public void setRequestCmd(String requestCmd) {
		this.requestCmd = requestCmd;
	}

	public String getRequestKey() {
		return requestKey;
	}

	public void setRequestKey(String requestKey) {
		this.requestKey = requestKey;
	}

	public int getRequestSize() {
		return requestSize;
	}

	public void setRequestSize(int requestSize) {
		this.requestSize = requestSize;
	}

	public long getRequestTimeMills() {
		return requestTimeMills;
	}

	public void setRequestTimeMills(long requestTimeMills) {
		this.requestTimeMills = requestTimeMills;
	}
	
	public boolean intercept(RouteResult routeResult) throws IOException {
		boolean result = false;
		RedisRequest request = routeResult.getRequests().get(0);
		byte handleType = request.getPolicy().getHandleType();
		
		// pubsub 连接不允许其他指令
		if ( handleType != CommandParse.PUBSUB_CMD || routeResult.getRequestType() != RedisRequestType.DEFAULT) {
			if (pubsub != null) {
				frontCon.write("-ERR not support commond to channel.\r\n".getBytes());
				result = true;
			}
		}
		switch (handleType) {
		case CommandParse.PUBSUB_CMD:
			return result || interceptPubsub(routeResult);
			
		case CommandParse.PARTITIONS_CMD:
			return result || interceptKpartitions(request);
			
		case CommandParse.PRIVATE_CMD:
			return result || interceptKafkaPrivateCmds(request);

		default:
			return result;
		}
		
	}
	
	// 拦截 PUBSUB
	private boolean interceptPubsub(RouteResult routeResult) throws IOException {
		RedisRequest request = routeResult.getRequests().get(0);
		
		boolean isIntercepted = false;
		
		RouteNode node = routeResult.getRouteNodes().get(0);
		
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		if ( cmd.startsWith("SUBSCRIBE") || cmd.startsWith("PSUBSCRIBE") ) {

			// SUBSCRIBE
			if ( pubsub != null && !pubsub.isClosed() ) {
				frontCon.write("-ERR Please unsubscribe to channel.\r\n".getBytes());
				
			} else {

				// PUBSUB
				pubsub = new PubSub(frontCon, new AbstractBackendCallback() {
					@Override
					public void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException {
						if (frontCon != null && !frontCon.isClosed()) {
							frontCon.write(byteBuff);
						}
	
						if ( pubsub != null ) {
							pubsub.close();
							pubsub = null;
						}
					}
				});
				pubsub.subscribe(request, node.getPhysicalNode());
			}
			
			isIntercepted = true;

		} else if ( cmd.startsWith("UNSUBSCRIBE") || cmd.startsWith("PUNSUBSCRIBE") ) {

			// UNSUBSCRIBE
			if (pubsub != null && !pubsub.isClosed()) {
				pubsub.unsubscribe(request);
			} else {
				frontCon.write("-ERR Please subscribe to channel.\r\n".getBytes());
			}
			
			isIntercepted = true;
		}
		
		return isIntercepted;
	}
	
	
	// 拦截 kapartitions
	private boolean interceptKpartitions(RedisRequest request) throws IOException {

		int poolId = frontCon.getUserCfg().getPoolId();
		String topic = new String(request.getArgs()[1]);
		KafkaPoolCfg poolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get(poolId);
		TopicCfg tc = poolCfg.getTopicCfgMap().get(topic);
		
		Collection<BrokerPartition> partitions = tc.getRunningInfo().getPartitions().values();

		// 申请1k buffer （肯定够）
		ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(1024);
		byte ASTERISK = '*';
		byte DOLLAR = '$';
		byte[] CRLF = "\r\n".getBytes();

		byte[] size = ProtoUtils.convertIntToByteArray(partitions.size());
		bb.put(ASTERISK).put(size).put(CRLF);
		for (BrokerPartition dataPartition : partitions) {
			byte[] partition = ProtoUtils.convertIntToByteArray(dataPartition.getPartition());
			byte[] partitionLength = ProtoUtils.convertIntToByteArray(partition.length);
			bb.put(DOLLAR).put(partitionLength).put(CRLF).put(partition).put(CRLF);
		}

		frontCon.write(bb);

		return true;
	}
	
	// 拦截内部调用指令
	private boolean interceptKafkaPrivateCmds(RedisRequest request) throws IOException {
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		String topic = new String(request.getArgs()[1]);
		int partition = Integer.parseInt(new String(request.getArgs()[2]));
		try {
			// 申请offset
			if ( cmd.equals("KGETOFFSET") ) {
				long offset = BrokerOffsetService.INSTANCE().getOffsetForSlave(frontCon.getPassword(), topic, partition);
				StringBuffer sb = new StringBuffer();
				sb.append("+").append(offset).append("\r\n");
				frontCon.write(sb.toString().getBytes());
				
			// 返还 offset
			} else if ( cmd.equals("KRETURNOFFSET") ) {
				long offset = Long.parseLong(new String(request.getArgs()[3]));
				BrokerOffsetService.INSTANCE().returnOffsetForSlave(frontCon.getPassword(), topic, partition, offset);
				frontCon.write(OK);
			}
		} catch (Exception e) {
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR").append(e.getMessage()).append("\r\n");
			frontCon.write(sb.toString().getBytes());
		}
		
		return true;
	}
	
	private void cleanup() {
		defaultCommandHandler = null;
		segmentCommandHandler = null;
		pipelineCommandHandler = null;
		blockCommandHandler = null;
		kafkaCommandHandler = null;
		currentCommandHandler = null;
	}
	
	// FRONT CONNECTION EVENT 
	// ---------------------------------------------------------------------------------------
	public void frontConnectionClose(String reason) {
		
		if ( pubsub != null ) 
			pubsub.unsubscribeAll();
		
		if ( currentCommandHandler != null )
			currentCommandHandler.frontConnectionClose(reason);
		
		this.cleanup();
	}
	
	// BACKEND CONNECTION EVENT 
	// ---------------------------------------------------------------------------------------
	public void backendConnectionError(Exception e) {
		
		if ( currentCommandHandler != null )
			currentCommandHandler.backendConnectionError(e);
		else
			frontCon.writeErrMessage(e.toString());
		
		frontCon.close("backend connectionError");
	}

	public void backendConnectionClose(String reason) {
		
		if ( currentCommandHandler != null )
			currentCommandHandler.backendConnectionClose(reason);
		else
			frontCon.writeErrMessage(reason);
		
		frontCon.close("backend connectionClose");
	}
	
}
