package com.feeyo.redis.net.front.subpackage.strategy;

public class SubPackageStrategyFactory {
	
	private static MSetSubPackageStrategy MSET_STR =  new MSetSubPackageStrategy();
	private static MGetSubPackageStrategy MGET_STR =  new MGetSubPackageStrategy();
	private static MDelSubPackageStrategy MDEL_STR =  new MDelSubPackageStrategy();
	private static MExistsSubPackageStrategy EXISTS_STR =  new MExistsSubPackageStrategy();
	

	public static SubPackageStrategy getSubPackageStrategy(String cmd) {
		if (cmd.startsWith("MSET"))
			return MSET_STR;
		else if (cmd.startsWith("MGET"))
			return MGET_STR;
		else if (cmd.startsWith("DEL"))
			return MDEL_STR;
		else if (cmd.startsWith("EXISTS"))
			return EXISTS_STR;
		return null;
	}

}
