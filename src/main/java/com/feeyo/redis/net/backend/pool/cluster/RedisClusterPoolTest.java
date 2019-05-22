package com.feeyo.redis.net.backend.pool.cluster;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.exception.JedisConnectionException;

public class RedisClusterPoolTest {
	
	public static final String LOCALHOST_STR = getLocalHostQuietly();
	
	
	public static void main(String args[]) {
		
		JedisConnection conn = null;
		try {
			String host = "127.0.0.1";
			int port = 6379;					
			conn = new JedisConnection(host, port);
			conn.sendCommand( RedisCommand.CLUSTER, "nodes");
			String nodeInfos = conn.getBulkReply();
			List<ClusterNode> nodes = parseClusterNodes( nodeInfos );	
			for(ClusterNode node: nodes) {
				System.out.println( node.toString() );
			}

		} catch (JedisConnectionException e) {
			e.printStackTrace();
			System.out.println("discover cluster node err:");	
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	private static List<ClusterNode> parseClusterNodes(String nodeInfos) {
		
		List<ClusterNode> nodes = new ArrayList<ClusterNode>();
		for (String nodeInfo : nodeInfos.split("\n")) {
             nodes.add( parseClusterNode(nodeInfo) );
        }		
		return nodes;		
	}
	
	
	private static ClusterNode parseClusterNode(String source) {
		String[] fields = source.split(" ");
		
		ClusterNode node = new ClusterNode();
		node.setId(fields[0]);
		
		String host = fields[1].split(":")[0];
		if (host.equals("127.0.0.1") 
				|| host.startsWith("localhost") 
				|| host.equals("0.0.0.0")
				|| host.startsWith("169.254") 
				|| host.startsWith("::1") 
				|| host.startsWith("0:0:0:0:0:0:0:1")) {
			node.setHost( LOCALHOST_STR );
		} else {
			node.setHost( host );
		}
		
		node.setPort(Integer.parseInt(fields[1].split(":")[1]));
		node.setFlags(fields[2]);
		node.setType(fields[2].indexOf("master") > -1 ? "master" : "slave");
		node.setLinkState(fields[7]);
		if ( !"-".equals(fields[3]) ) {
			node.setMasterId(fields[3]);
		}
		
		// 多个slot 段位处理
		if (fields.length > 8) {
			int slotIndex = 8;
			while ( slotIndex  < fields.length ) {
				node.getSlotRangeList().addAll( parseSlot( fields[ slotIndex ] ) );
				slotIndex++;
			}
		}
		return node;
	}
	 
	 private static String getLocalHostQuietly() {
		String localAddress;
		try {
			localAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			localAddress = "localhost";
		}
		return localAddress;
	}
	
	
	//convert 8192-12287,xxx-xxx to a List<SlotRange>
	private static List<SlotRange> parseSlot(String source) {

		List<SlotRange> list = new ArrayList<SlotRange>();
		
		String[] slotRangeArray = source.split(",");
		for (String slotRange : slotRangeArray) {
			String[] slot = slotRange.split("-");
			list.add(new SlotRange(new Integer(slot[0]), new Integer(slot[1])));
		}		 
		return list;		 
	}
	
	
	// node
	static class ClusterNode {
		
		private String id;
		private String type;
		private String host;
		private int port;
		private String masterId;
		private String flags;
		private String linkState;
		private List<SlotRange> slotRangeList = new ArrayList<SlotRange>();

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

		public String getFlags() {
			return flags;
		}

		public void setFlags(String flags) {
			this.flags = flags;
		}

		public String getLinkState() {
			return linkState;
		}

		public void setLinkState(String linkState) {
			this.linkState = linkState;
		}

		public List<SlotRange> getSlotRangeList() {
			return slotRangeList;
		}

		public void setSlotRangeList(List<SlotRange> slotRangeList) {
			this.slotRangeList = slotRangeList;
		}
		
		@Override
		public String toString() {
			
			
			final StringBuilder sbuf = new StringBuilder("ClusterNode[")
					.append("id=").append( id ).append(',')
					.append("type=").append( type ).append(',')
					.append("host=").append( host ).append(',')
					.append("port=").append( port ).append(',')
					.append("masterId=").append( masterId ).append(',')
					.append("flags=").append( flags ).append(',')
					.append("linkState=").append( linkState ).append(',')
					
					.append("slots=").append('[');
					if ( slotRangeList != null ){
						for(SlotRange slotRange: slotRangeList) {
							sbuf.append( slotRange.toString() );
						}
					}					
					sbuf.append(']')
					.append(']');
					return (sbuf.toString());
		}
		
	}
	
	
	// slot
	static class SlotRange {
		
		private Integer start;
		private Integer end;
		
		public SlotRange(Integer start, Integer end) {
			super();
			this.start = start;
			this.end = end;
		}

		public Integer getStart() {
			return start;
		}

		public Integer getEnd() {
			return end;
		}
		
		@Override
		public String toString() {
			return "start=" + start + ", end=" + end;
		}
	}

}
