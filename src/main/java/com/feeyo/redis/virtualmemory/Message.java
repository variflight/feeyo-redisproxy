package com.feeyo.redis.virtualmemory;

import java.io.Serializable;


public class Message implements Serializable {
	
	private static final long serialVersionUID = 7849892653730088375L;
	
	private long queueId;
	private int sysFlag;
	private long bornTimestamp;
	private long storeTimestamp;
	private long commitLogOffset;
	private int bodyCRC;
    private byte[] body;
    private int storeSize;

    public Message() {
    }

    public Message(long queueId, byte[] body) {
        this(queueId, 0, body);
    }

    public Message(long queueId, int sysFlag, byte[] body) {
      this.queueId = queueId;
      this.sysFlag = sysFlag;
        this.body = body;
    }

	public long getQueueId() {
		return queueId;
	}

	public void setQueueId(long queueId) {
		this.queueId = queueId;
	}

	public int getSysFlag() {
		return sysFlag;
	}

	public void setSysFlag(int sysFlag) {
		this.sysFlag = sysFlag;
	}

	public long getBornTimestamp() {
		return bornTimestamp;
	}

	public void setBornTimestamp(long bornTimestamp) {
		this.bornTimestamp = bornTimestamp;
	}

	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	public void setStoreTimestamp(long storeTimestamp) {
		this.storeTimestamp = storeTimestamp;
	}

	public long getCommitLogOffset() {
		return commitLogOffset;
	}

	public void setCommitLogOffset(long commitLogOffset) {
		this.commitLogOffset = commitLogOffset;
	}

	public int getBodyCRC() {
		return bodyCRC;
	}

	public void setBodyCRC(int bodyCRC) {
		this.bodyCRC = bodyCRC;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public int getStoreSize() {
		return storeSize;
	}

	public void setStoreSize(int storeSize) {
		this.storeSize = storeSize;
	}
	
	

}
