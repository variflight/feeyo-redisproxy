package com.feeyo.redis.net.backend.pool.cluster;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.net.backend.pool.PhysicalNode;

/**
 *  Cluster Node
 */
public class ClusterNode {
	
	private PhysicalNode physicalNode = null;
	
	private String id;
	private String type;		
	private String host;
	private int port;		
	private String masterId;
	private String flagInfo;				// fail ...  myself,master 
	private String connectInfo;				// "connected" , "disconnected"
	
	private boolean isFail = false;			// 可用状态
	private boolean isConnected = true;		// 连接状态
	
	private long pingSentTs;
	private long pongReceivedTs;
	
	private List<SlotRange> slotRanges = new ArrayList<SlotRange>();

	public PhysicalNode getPhysicalNode() {
		return physicalNode;
	}

	public void setPhysicalNode(PhysicalNode node) {
		this.physicalNode = node;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getMasterId() {
		return masterId;
	}

	public void setMasterId(String masterId) {
		this.masterId = masterId;
	}

	public String getFlagInfo() {
		return flagInfo;
	}

	public void setFlagInfo(String flagInfo) {
		this.flagInfo = flagInfo;
	}

	public String getConnectInfo() {
		return connectInfo;
	}

	public void setConnectInfo(String connectInfo) {
		this.connectInfo = connectInfo;
	}
	
	public boolean isFail() {
		return isFail;
	}

	public void setFail(boolean isFail) {
		this.isFail = isFail;
	}

	public boolean isConnected() {
		return isConnected;
	}

	public void setConnected(boolean isConnected) {
		this.isConnected = isConnected;
	}

	public long getPingSentTs() {
		return pingSentTs;
	}

	public void setPingSentTs(long pingSentTs) {
		this.pingSentTs = pingSentTs;
	}

	public long getPongReceivedTs() {
		return pongReceivedTs;
	}

	public void setPongReceivedTs(long pongReceivedTs) {
		this.pongReceivedTs = pongReceivedTs;
	}

	public List<SlotRange> getSlotRanges() {
		return slotRanges;
	}

	public void setSlotRanges(List<SlotRange> slotRanges) {
		this.slotRanges = slotRanges;
	}
	
	@Override
	public String toString() {
		final StringBuilder sbuf = new StringBuilder("ClusterNode[")
				.append("id=").append( id ).append(',')
				.append("type=").append( type ).append(',')
				.append("host=").append( host ).append(',')
				.append("port=").append( port ).append(',')
				.append("masterId=").append( masterId ).append(',')
				.append("flagInfo=").append( flagInfo ).append(',')
				.append("connectInfo=").append( connectInfo ).append(',')
				.append("pingSentTs=").append( pingSentTs ).append(',')
				.append("pongReceivedTs=").append( pongReceivedTs );
				
				if ( slotRanges != null ){
					sbuf.append("slot=[ ");
					for(SlotRange r : slotRanges) {
						sbuf.append( r.getStart() ).append("-").append( r.getEnd() ).append(" ");
					}
					sbuf.append("]");
				}				
				sbuf.append(" ]");
				return (sbuf.toString());
	}
}
