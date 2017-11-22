package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestDecoderV5;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.engine.codec.RedisRequestUnknowException;
import com.feeyo.redis.engine.manage.Manage;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.front.handler.AbstractCommandHandler;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.handler.DefaultCommandHandler;
import com.feeyo.redis.net.front.handler.SegmentCommandHandler;
import com.feeyo.redis.net.front.handler.PipelineCommandHandler;
import com.feeyo.redis.net.front.handler.PubSub;
import com.feeyo.redis.net.front.route.AutoRespNotTransException;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.ManageRespNotTransException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.net.front.route.RouteService;

public class RedisFrontSession {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisFrontSession.class );

	public static final Charset _charset = Charset.forName( "UTF-8" );
	public static final byte[] OK =   "+OK\r\n".getBytes();
	public static final byte[] PONG =  "+PONG\r\n".getBytes();
	public static final byte[] ERR_NO_AUTH = "-NOAUTH Authentication required.\r\n".getBytes();	
	public static final byte[] ERR_NO_AUTH_PASSWORD = "-ERR invalid password.\r\n".getBytes();
	public static final byte[] ERR_NO_AUTH_NO_PASSWORD = "-ERR Client sent AUTH, but no password is set\r\n".getBytes();
	public static final byte[] ERR_PIPELINE_BACKEND = "-ERR pipeline error\r\n".getBytes();
	
	public static final String NOT_SUPPORTED = "Not supported.";
	public static final String NOT_ADMIN_USER = "Not supported:manage cmd but not admin user.";
	public static final String UNKNOW_COMMAND = "Unknow command.";
	public static final String NOT_READ_CMD = "Not read cmd.";
	
	// PUBSUB
	private PubSub pubsub = null;
	
	// 
	private String requestCmd;
	private byte[] requestKey;
	private int requestSize;
	private long requestTimeMills; 
	
    // 解析器 
	private RedisRequestDecoderV5 requestDecoder = new RedisRequestDecoderV5();
	
	private AbstractCommandHandler defaultCommandHandler;
	private AbstractCommandHandler multiOperatorCommandHandler;
	private AbstractCommandHandler pipelineCommandHandler;
	
	private AbstractCommandHandler currentCommandHandler;
	
	private Object _lock = new Object();
	
	private RedisFrontConnection frontCon;

	public RedisFrontSession(RedisFrontConnection frontCon) {
		this.frontCon = frontCon;
	}

	public void handle(byte[] byteBuff) {
		
		// 默认需要立即释放
		boolean isImmediateReleaseConReadLock = true;
		
		try {
			// parse
			List<RedisRequest> requests = requestDecoder.decode(byteBuff);
			if (requests == null ) {
				return;
			}
			
			// 非pipeline 情况下， 特殊指令前置优化性能
			if ( requests.size() ==  1 ) {
				
				RedisRequest request = requests.get(0);
				byte[] cmd = request.getArgs()[0];
				int len = cmd.length;
				if ( len == 4 ) {
					
					// AUTH
					if ( (cmd[0] == 'A' || cmd[0] == 'a') && (cmd[1] == 'U' || cmd[1] == 'u') 
							&& (cmd[2] == 'T' || cmd[2] == 't') && (cmd[3] == 'H' || cmd[3] == 'h')   ) {
						
						if( request.getArgs().length < 2 ) {
							frontCon.write( ERR_NO_AUTH_NO_PASSWORD );
							return;
						}
						
						auth( request );
						return;
					
					// ECHO
					} else if ( (cmd[0] == 'E' || cmd[0] == 'e') && (cmd[1] == 'C' || cmd[1] == 'c') 
							 && (cmd[2] == 'H' || cmd[2] == 'h') && (cmd[3] == 'O' || cmd[3] == 'o') ) {
						echo( request );
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
				
				RedisRequest firstRequest = requests.get(0);
				byte[] cmd = firstRequest.getArgs()[0];
				if (cmd.length == 4 && 
						(cmd[0] == 'A' || cmd[0] == 'a') && 
						(cmd[1] == 'U' || cmd[1] == 'u') && 
						(cmd[2] == 'T' || cmd[2] == 't') && 
						(cmd[3] == 'H' || cmd[3] == 'h')) {

					auth( firstRequest );
					requests.remove(0);
					if ( requests.isEmpty() ) {
						return;
					}

				} else {
					frontCon.write(ERR_NO_AUTH);
					return;
				}	
			} 
			
			// 执行路由
			try {

				RouteResult routeResult = RouteService.route(requests, frontCon);
				boolean intercepted = interceptPubsub( routeResult );
				if ( intercepted ) {
					return;
				}
				
				currentCommandHandler = this.getCommandHandler( routeResult.getRequestType() );
				currentCommandHandler.handle(routeResult);
				
				if ( routeResult.getRequestType() != RedisRequestType.DEFAULT ) {
					// pipeline mget mset mdel 暂时不释放锁
					isImmediateReleaseConReadLock = false;
				}
				
			} catch (InvalidRequestExistsException e) {
				
				// 指令策略未通过
				List<RedisRequestPolicy> requestPolicys = e.getRequestPolicys();
				if ( requestPolicys != null ) {
					
					if ( requestPolicys.size() > 1 ) {
						
						StringBuffer sb = new StringBuffer();
						sb.append("-ERR ");
						for (int i = 0; i < requestPolicys.size(); i++) {
							String resp = getInvalidCmdResponse(requestPolicys.get(i), frontCon.getUserCfg().isAdmin());
							if (resp != null) {
								sb.append("NO: ").append(i+1).append(", ").append(resp);
							}
						}
						sb.append("\r\n");
						frontCon.write( sb.toString().getBytes() );
						
					} else {
						// 此处用于兼容
						frontCon.write( OK );
					}
					
				} else {
					frontCon.write( ("-ERR " + e.getMessage()+"\r\n").getBytes() );
				}
				
				LOGGER.warn("con: {}, request err: {}", this.frontCon, requests);
				
			} catch(ManageRespNotTransException e) {
				
				// 管理指令
				RedisRequest request = e.getRequests().get(0);
				byte[] buff = Manage.execute(request, frontCon);
				if (buff != null)
					frontCon.write(buff);

			} catch (AutoRespNotTransException e) {

				//  自动响应指令
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
				
			} catch (PhysicalNodeUnavailableException e) {
				//-ERR node unavaliable error \r\n
				frontCon.write( "-ERR node unavailable error \r\n".getBytes() );
			}
			
		} catch (RedisRequestUnknowException e0) {
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
			if (multiOperatorCommandHandler == null) {
				synchronized (_lock) {
					if (multiOperatorCommandHandler == null) {
						multiOperatorCommandHandler = new SegmentCommandHandler( frontCon );
					}
				}
			}
			return multiOperatorCommandHandler;
			
		case PIPELINE:
			if (pipelineCommandHandler == null) {
				synchronized (_lock) {
					if (pipelineCommandHandler == null) {
						pipelineCommandHandler = new PipelineCommandHandler( frontCon );
					}
				}
			}
			return pipelineCommandHandler;

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
	
	
	
	
	// 无效指令应答
	private String getInvalidCmdResponse(RedisRequestPolicy policy, boolean isAdmin) {
		
		String result = null;
		
		switch( policy.getLevel() ) {
		case CommandParse.NO_CLUSTER_CMD:
			if ( frontCon.getUserCfg().getPoolType() == 1 ) 
				result = NOT_SUPPORTED;
			break;
		case CommandParse.CLUSTER_CMD:
			if ( frontCon.getUserCfg().getPoolType() == 0 )
				result = NOT_SUPPORTED;
			break;
		case CommandParse.DISABLED_CMD:
		case CommandParse.PUBSUB_CMD:
		case CommandParse.MGETSET_CMD:
			result = NOT_SUPPORTED;
			break;
		case CommandParse.MANAGE_CMD:
			if (isAdmin) {
				result = NOT_SUPPORTED;
			} else {
				result = NOT_ADMIN_USER;
			}
			break;
		case CommandParse.UNKNOW_CMD:
			result = UNKNOW_COMMAND;
			break;
		default:
			result = NOT_SUPPORTED;
			break; 
		}
		
		// ReadOnly， 不能执行写入操作
		if ( frontCon.getUserCfg().isReadonly() && !policy.isRead() )  {
			result = NOT_READ_CMD;
		}
		return result;
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

	public byte[] getRequestKey() {
		return requestKey;
	}

	public void setRequestKey(byte[] requestKey) {
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
	
	
	// 拦截 PUBSUB
	public boolean interceptPubsub(RouteResult routeResult) throws IOException {
		
		
		if ( routeResult.getRequestType() != RedisRequestType.DEFAULT  ) {
			
			if ( pubsub == null ) {
				return false;
			}
			
			frontCon.write("-ERR not support pipeline to channel.\r\n".getBytes());
			return true;
			
		}
		
		boolean isIntercepted = false;
		
		//
		RouteResultNode node = routeResult.getRouteResultNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		if ( cmd.startsWith("SUBSCRIBE") || cmd.startsWith("PSUBSCRIBE") ) {

			// SUBSCRIBE
			if ( pubsub != null && !pubsub.isClosed() ) {
				frontCon.write("-ERR Please unsubscribe to channel.\r\n".getBytes());
				
			} else {

				// PUBSUB
				pubsub = new PubSub(frontCon, new AbstractBackendCallback() {
					@Override
					public void handleResponse(RedisBackendConnection conn, byte[] byteBuff) throws IOException {
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
	
	
	private void cleanup() {
		defaultCommandHandler = null;
		multiOperatorCommandHandler = null;
		pipelineCommandHandler = null;
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
