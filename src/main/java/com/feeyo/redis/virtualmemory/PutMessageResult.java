package com.feeyo.redis.virtualmemory;


/**
 * 写入消息返回结果
 *
 */
public class PutMessageResult {
	
	// 写入消息过程的返回结果
	public enum PutMessageStatus {
		PUT_OK,
	    SERVICE_NOT_AVAILABLE,
	    CREATE_MAPEDFILE_FAILED,
	    MESSAGE_ILLEGAL,
	    OS_PAGECACHE_BUSY,
	    UNKNOWN_ERROR
	}
	
	
    private PutMessageStatus putMessageStatus;
    private AppendMessageResult appendMessageResult;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    public boolean isOk() {
        return  this.putMessageStatus != null && putMessageStatus == PutMessageStatus.PUT_OK 
        		&& this.appendMessageResult != null && this.appendMessageResult.isOk();
    }

    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }

    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }

    @Override
    public String toString() {
        return "PutMessageResult [putMessageStatus=" + putMessageStatus + ", appendMessageResult="
            + appendMessageResult + "]";
    }

}