package com.feeyo.kafka.net.backend.broker.zk.running;


import java.io.Serializable;


/**
 * 服务端running状态信息
 */
public class ServerRunningData implements Serializable {

    private static final long serialVersionUID = 92260481691855281L;

    private String            address;
    private boolean           active           = true;
    private String			 ip;
    private int               port;

    public ServerRunningData(){
    }

    public ServerRunningData(String address){
        this.address = address;
    		String[] strs = address.split(":");
    		this.ip = strs[0];
    		this.port = Integer.parseInt(strs[1]);
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
