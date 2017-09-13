package com.feeyo.redis.virtualmemory;

/**
 * When write a message to the commit log, returns results
 */
public class AppendMessageResult {
	
	public enum AppendMessageStatus {
	    PUT_OK,
	    END_OF_FILE,
	    MESSAGE_SIZE_EXCEEDED,
	    UNKNOWN_ERROR,
	}
	
	// Return code
	private AppendMessageStatus status;
	
	// Where to start writing
	private long wroteOffset;	
	
	// Write Bytes
	private int wroteBytes;
	
    // Message storage timestamp
	private long storeTimestamp;
	
	private long pagecacheRT = 0;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, 0, 0);
    }

	public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes,
			long storeTimestamp,  long pagecacheRT) {
		this.status = status;
		this.wroteOffset = wroteOffset;
		this.wroteBytes = wroteBytes;
		this.storeTimestamp = storeTimestamp;
		this.pagecacheRT = pagecacheRT;
	}

	public long getPagecacheRT() {
		return pagecacheRT;
	}

	public void setPagecacheRT(final long pagecacheRT) {
		this.pagecacheRT = pagecacheRT;
	}

	public boolean isOk() {
		return this.status == AppendMessageStatus.PUT_OK;
	}

	public AppendMessageStatus getStatus() {
		return status;
	}

	public void setStatus(AppendMessageStatus status) {
		this.status = status;
	}

	public long getWroteOffset() {
		return wroteOffset;
	}

	public void setWroteOffset(long wroteOffset) {
		this.wroteOffset = wroteOffset;
	}

	public int getWroteBytes() {
		return wroteBytes;
	}

	public void setWroteBytes(int wroteBytes) {
		this.wroteBytes = wroteBytes;
	}
	
	public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
            "status=" + status +
            ", wroteOffset=" + wroteOffset +
            ", wroteBytes=" + wroteBytes +
            ", storeTimestamp=" + storeTimestamp +
            ", pagecacheRT=" + pagecacheRT +
            '}';
    }
}
