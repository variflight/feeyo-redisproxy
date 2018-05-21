package com.feeyo.kafka.net.backend.broker.zk.running;


import java.io.Serializable;


/**
 * 服务端running状态信息
 */
public class ServerRunningData implements Serializable {

    private static final long serialVersionUID = 92260481691855281L;

    private String            address;
    private boolean           active           = true;

    public ServerRunningData(){
    }

    public ServerRunningData(String address){
        this.address = address;
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

}
