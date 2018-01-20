package com.feeyo.redis.engine.manage.stat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.engine.manage.stat.BigLengthCollector.BigLength;
import com.feeyo.util.FileUtils;
import com.feeyo.util.MailUtil;

public class StatNotification {
	
	//附件
	private static final String[] fileNames = new String[] { "big_key.txt", "big_length.txt" };
	
	
	private static List<String> attachmentFileNames = new ArrayList<String>();
	
	public void gatherStatusContent() {
		
		ConcurrentHashMap<String, BigKey> bigkeyStats = StatUtil.getBigKeyMap();
		StringBuffer tableBuffer = new StringBuffer();
		tableBuffer.append("#############   bigkey status   #################\n");
		tableBuffer.append("|    cmd    |     key     |    size    |    count    |");

		// 获取bigkey的数据信息
		for (BigKey bigkey : bigkeyStats.values()) {
			tableBuffer.append("\n");
			tableBuffer.append("|    ");
			tableBuffer.append(bigkey.cmd).append("    |    ");
			tableBuffer.append(bigkey.key).append("    |    ");
			tableBuffer.append(bigkey.size).append("    |    ");
			tableBuffer.append(bigkey.count).append("    |");
		}

		String[] attachmentsNames = transFilePathName(fileNames);
		FileUtils.ensureCreateFile(attachmentsNames[0], tableBuffer.toString(),false);
		attachmentFileNames.add(attachmentsNames[0]);
		
		tableBuffer.setLength(0);

		tableBuffer.append("#############   top  hundred   #################\n");
		tableBuffer.append("|    key    |     type     |    length    |    count_1k    |    count_10k    |");
		for (BigLength bigLength : StatUtil.getBigLengthMap().values()) {
			tableBuffer.append("\n");
			tableBuffer.append("|    ");
			tableBuffer.append(bigLength.key).append("    |    ");
			tableBuffer.append(bigLength.cmd).append("    |    ");
			tableBuffer.append(bigLength.length).append("    |    ");
			tableBuffer.append(bigLength.count_1k).append("    |    ");
			tableBuffer.append(bigLength.count_10k).append("    |");
		}

		FileUtils.ensureCreateFile(attachmentsNames[1], tableBuffer.toString(),false);
		attachmentFileNames.add(attachmentsNames[1]);
		
	}
	
	public void gatherDailyDiscardContent() {
		String basepath = System.getProperty("FEEYO_HOME")+"\\store\\discard\\";
		String[] filenames = FileUtils.getFilenamesUnderDir(basepath);
		attachmentFileNames.addAll(filterFilenames(filenames));
	}
	
	public void sendMail() {
		//
		try {

			StringBuffer body = new StringBuffer();
			body.append("");
			body.append("");
			body.append("");
			body.append("");
			MailUtil.send(getMailProperty(), " ## RedisProxy Report ##", body.toString(), attachmentFileNames.toArray(new String[0]));
			//clean
			attachmentFileNames.clear();
		} finally {
			FileUtils.cleanFiles(attachmentFileNames.toArray(new String[0]));
		}
	}
	
	private String[] transFilePathName(String[] filenames) {
		
		String filePath = System.getProperty("FEEYO_HOME") + "\\store\\";
		
		String[] attachementNames = new String[filenames.length];
		for(int index = 0 ;index< filenames.length; index++) {
			attachementNames[index] = filePath + getYesterdayDate()+"_"+filenames[index];
		}
		return attachementNames;
	}
	
	private List<String> filterFilenames(String[] filenames) {
		List<String> list = new ArrayList<String>();
		
		for(String filename : filenames) {
			String date = getYesterdayDate();
			if(filename.indexOf(date)>= 0)
				list.add(filename);
		}
		return list;
	}
	
	//由于是0点发送邮件，所以取昨天的日期
	private String getYesterdayDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -1);
		return sdf.format(cal.getTime());
	}
	
	private Properties getMailProperty() {
		return RedisEngineCtx.INSTANCE().getMailProperties();
	}
}
