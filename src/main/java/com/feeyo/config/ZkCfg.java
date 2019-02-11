package com.feeyo.config;

import com.google.common.base.Joiner;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * the configuration for {@link com.feeyo.redis.config.loader.zk.ZkClient}
 *
 * @author Tr!bf wangyamin@variflight.com
 * @see com.feeyo.redis.config.loader.zk.ZkClient
 *
 */
public class ZkCfg {
    private boolean usingZk;    // 是否启用 ZK
    private boolean autoAct;    // 是否自动激活
    private String zkHome;      // 配置文件在 zk 中的目录位置
    private String instanceIdPath;
    private List<String/*ip:port*/> zkServers;
    private List<String/*local configure file name, NOT path*/> locCfgNames;

    public ZkCfg() {
        zkServers = new ArrayList<>();
        locCfgNames = new ArrayList<>();
    }

    public List<String> getZkServers() {
        return zkServers;
    }

    public List<String> getLocCfgNames() {
        return locCfgNames;
    }

    public boolean isUsingZk() {
        return usingZk;
    }

    public void setUsingZk(boolean usingZk) {
        this.usingZk = usingZk;
    }

    public boolean isAutoAct() {
        return autoAct;
    }

    public void setAutoAct(boolean autoAct) {
        this.autoAct = autoAct;
    }

    public String getZkHome() {
        return zkHome;
    }

    public void setZkHome(String zkHome) {
        this.zkHome = zkHome;
    }

    public String getInstanceIdPath() {
        return instanceIdPath;
    }

    public void setInstanceIdPath(String instanceIdPath) {
        this.instanceIdPath =  zkHome + "instance/" + instanceIdPath;
    }

    public String buildZkPathFor(String cfgName) {
        return zkHome + "conf/" + cfgName;
    }

    public String buildCfgBackupPathFor(String cfgName) {
        return System.getProperty("FEEYO_HOME") + File.separator + "conf"+ File.separator + "bak" + File.separator + cfgName;
    }

    public String buildConnectionString() {
        return Joiner.on(",").join(zkServers);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (this.getClass() != obj.getClass())
            return false;

        ZkCfg other = (ZkCfg) obj;
        if ( this.usingZk == other.isUsingZk() &&
                this.autoAct == other.isAutoAct() &&
                this.zkHome.equals(other.getZkHome()) &&
                Arrays.deepEquals(zkServers.toArray(),other.getZkServers().toArray()) &&
                Arrays.deepEquals(locCfgNames.toArray(),other.getLocCfgNames().toArray()) ) {

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return " usingZk: " + usingZk +
                "\n autoAct: " + autoAct +
                "\n zkHome: " + zkHome +
                "\n instanceIdPath: " + instanceIdPath +
                "\n zkServers: " + zkServers +
                "\n locCfgNames: " + locCfgNames;
    }
}
