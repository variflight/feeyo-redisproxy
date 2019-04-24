package com.feeyo.redis.config;

import java.util.Arrays;
import java.util.regex.Pattern;

public class UserCfg {
	
	private final int poolId;
	private final int poolType;
	
	private final String password;
	private final byte[] prefix;		// 前缀
	private int selectDb;				// 库
	private final int maxCon;			// 最大连接数
	
	private final boolean isAdmin;
	private final boolean isReadonly;
	private final Pattern keyRule;
	
	
	
	// 通过管理指令 use pool 改变
	//
	private int usePoolId;
	private int usePoolType = -1;

	
	public UserCfg(int poolId, int poolType, String password,  String prefix, 
			int selectDb, int maxCon, boolean isAdmin, boolean isReadonly, Pattern keyRule) {
		this.poolId = poolId;
		this.poolType = poolType;
		this.password = password;		
		this.prefix = prefix == null ? null : prefix.getBytes();
		this.selectDb = selectDb;
		this.maxCon = maxCon;
		this.isAdmin = isAdmin;
		this.isReadonly = isReadonly;
		
		this.usePoolId = poolId;
		this.usePoolType = poolType;
		this.keyRule = keyRule;
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

	public int getMaxCon() {
		return maxCon;
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

	public Pattern getKeyRule() {
		return keyRule;
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
