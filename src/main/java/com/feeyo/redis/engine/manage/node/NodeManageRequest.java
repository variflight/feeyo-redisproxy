package com.feeyo.redis.engine.manage.node;

/**
 * 
 * @author yangtao
 *
 */
public class NodeManageRequest {
	private CmdType cmdType;
	private String ip;
	private int port;
	private int slots;
	private String id;
	
	public NodeManageRequest(CmdType cmdType, String ip, int port) {
		this.cmdType = cmdType;
		this.ip = ip;
		this.port = port;
	}
	
	enum CmdType {
		NODE_CHECK,
		NODE_ADDMASTER,
		NODE_ADDSLAVE,
		NODE_DELETE,
		NODE_SLOTE_RESHARD
	}

	public CmdType getCmdType() {
		return cmdType;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public int getSlots() {
		return slots;
	}

	public void setSlots(int slots) {
		this.slots = slots;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
