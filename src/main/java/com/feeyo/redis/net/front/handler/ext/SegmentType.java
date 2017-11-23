package com.feeyo.redis.net.front.handler.ext;

//fragment
public enum SegmentType {
	MGET,  //由pipeline中MGET拆分出来的命令标志
	MSET,  //由pipeline中MSET拆分出来的命令标志
	MDEL,  //由pipeline中DEL批量操作拆分出来的命令标志
	MEXISTS, //由pipeline中EXISTS批量操作拆分出来的命令标志
	DEFAULT  //pipeline中的其他命令（非批量操作命令）
}
