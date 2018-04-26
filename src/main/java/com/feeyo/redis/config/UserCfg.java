package com.feeyo.redis.config;

import java.util.Arrays;

public class UserCfg {
	
	private int poolId;
	private int poolType = -1;
	
	private String password;
	private byte[] prefix;		//前缀
	private int selectDb;
	private boolean isAdmin = false;
	private boolean isReadonly = false;
	
	// 通过管理指令 use pool 改变
	private int usePoolId;
	private int usePoolType = -1;
	private UserFlowLimitCfg limitCfg;
	
	public UserCfg(int poolId, int poolType, String password,  String prefix, 
			int selectDb, boolean isAdmin, boolean isReadonly, int throughPercentage) {
		super();
		this.poolId = poolId;
		this.poolType = poolType;
		this.password = password;		
		this.prefix = prefix == null ? null : prefix.getBytes();
		this.selectDb = selectDb;
		this.isAdmin = isAdmin;
		this.isReadonly = isReadonly;
		
		this.usePoolId = poolId;
		this.usePoolType = poolType;
		
		if (throughPercentage == 100 || isAdmin) {
			limitCfg = null;
		} else {
			limitCfg = new UserFlowLimitCfg(throughPercentage);
		}
	}
	

	public int getPoolId() {
		return isAdmin ? usePoolId : poolId;
	}
	
	public int getPoolType() {
		return isAdmin ? usePoolType : poolType;
	}

	public String getPassword() {
		return password;
	}
	
	public byte[] getPrefix() {
		return prefix;
	}

	public int getSelectDb() {
		if (selectDb < 0 || selectDb > 12) {
			selectDb = 0;
		}
		return selectDb;
	}

	public boolean isAdmin() {
		return isAdmin;
	}
	
	public boolean isReadonly() {
		return isReadonly;
	}

	public void setUsePool(int poolId, int poolType) {
		this.usePoolId = poolId;
		this.usePoolType = poolType;
	}
	
	public UserFlowLimitCfg getLimitCfg() {
		return this.limitCfg;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((poolId == 0) ? 0 : poolId);
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
		result = prime * result + ((selectDb == 0) ? 0 : selectDb);
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
	    if (this == obj)
	        return true;
	    
	    if (obj == null)
	        return false;
	    
	    if (this.getClass() != obj.getClass())
	        return false;
	    
	    UserCfg other = (UserCfg) obj;
	    
	    if (other.getPassword().equals(password)
	            && other.getPoolId() == poolId
	            && other.getPoolType() == poolType
	            && other.getSelectDb() == selectDb
	            && Arrays.equals(other.getPrefix(), prefix)) {
	        return true;
	    } else {
	        return false;
	    }
	}
}
