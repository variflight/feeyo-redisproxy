package com.feeyo.redis.engine.manage.stat;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public abstract class AbstractStatCollector implements StatCollector{
	
	protected static final String FILE_TYPE = ".txt";
	protected static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
	protected static final String LINE_SPARATOR = System.getProperty("line.separator");
	protected static final String FIELD_SPARATOR = "  |  ";
	protected static String basepath;
	static{
		if( null == System.getProperty("FEEYO_HOME"))
			System.setProperty("FEEYO_HOME",System.getProperty("user.dir"));
		basepath = System.getProperty("FEEYO_HOME")+"\\store\\discard\\";
	}
	
	@Override
	public void onCollect(String host, String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
                          int waitTimeMills, boolean isCommandOnly, boolean isBypass) {
	}

	@Override
	public void onScheduleToZore() {
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	//临时文件以append方式
	protected abstract void saveFile(String date, boolean isTemp);
	
	protected String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return sdf.format(cal.getTime());
	}
}
