package com.feeyo.redis.net.backend.pool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.nio.Connection;
import com.feeyo.redis.nio.NetSystem;

public class ConQueue {

	//后端连接
	private final ConcurrentLinkedQueue<RedisBackendConnection> cons = new ConcurrentLinkedQueue<RedisBackendConnection>();

	public RedisBackendConnection takeIdleCon() {		
		ConcurrentLinkedQueue<RedisBackendConnection> f1 = cons;
		RedisBackendConnection con = f1.poll();
		if (con == null || con.isClosed() || !con.isConnected() ) {
			return null;
		} else {
			return con;
		}
	}

	public void removeCon(RedisBackendConnection con) {
		cons.remove(con);
	}

	public ConcurrentLinkedQueue<RedisBackendConnection> getCons() {
		return cons;
	}

	public ArrayList<RedisBackendConnection> getIdleConsToClose(int count) {
		ArrayList<RedisBackendConnection> readyCloseCons = new ArrayList<RedisBackendConnection>(count);
		while (!cons.isEmpty() && readyCloseCons.size() < count) {
			RedisBackendConnection theCon = cons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}
		return readyCloseCons;
	}	
	
	public int getActiveCountForNode(PhysicalNode node) {
        int total = 0;
        for (Connection conn : NetSystem.getInstance().getAllConnectios().values()) {
            if (conn instanceof RedisBackendConnection) {
            	RedisBackendConnection theCon = (RedisBackendConnection) conn;
                if (theCon.getPhysicalNode() == node) {
                    if (theCon.isBorrowed()) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    public void clearConnections(String reason, PhysicalNode node) {
        Iterator<Entry<Long, Connection>> itor = NetSystem.getInstance().getAllConnectios().entrySet().iterator();
        while ( itor.hasNext() ) {
            Entry<Long, Connection> entry = itor.next();
            Connection con = entry.getValue();
            if (con instanceof RedisBackendConnection) {
                if (((RedisBackendConnection) con).getPhysicalNode() == node) {
                    con.close(reason);
                    itor.remove();
                }
            }
        }
    }
    
    public void setIdleTimeConnections(PhysicalNode node, long idleTimeout) {    	
        Iterator<Entry<Long, Connection>> itor = NetSystem.getInstance().getAllConnectios().entrySet().iterator();
        while ( itor.hasNext() ) {
            Entry<Long, Connection> entry = itor.next();
            Connection con = entry.getValue();
            if (con instanceof RedisBackendConnection) {
                if (((RedisBackendConnection) con).getPhysicalNode() == node) {
                	con.setIdleTimeout( idleTimeout );
                }
            }
        }
    }
}