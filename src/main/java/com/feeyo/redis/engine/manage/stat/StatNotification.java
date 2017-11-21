package com.feeyo.redis.engine.manage.stat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.engine.manage.stat.BigLengthCollector.BigLength;
import com.feeyo.util.MailUtil;

public class StatNotification {
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
	
	//附件
	private static final String[] attachmentFileNames = new String[] { "big_key.txt", "big_length.txt" };
	
	private void createFile(String fileName, String content) {
		FileOutputStream fos = null;
		OutputStreamWriter out = null;
		BufferedWriter bw = null;
		try {
			File file =new File(fileName);
	        if(!file.exists()){  
	        	file.createNewFile();
	        } 
			fos = new FileOutputStream(file,false);
			out =new OutputStreamWriter(fos,"utf-8");
	        bw = new BufferedWriter(out);
	        bw.write(content);
	        bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	private String[] transFilePathName(String[] filenames) {
		
		String filePath = System.getProperty("FEEYO_HOME") + "\\store\\";
		
		String[] attachementNames = new String[filenames.length];
		for(int index = 0 ;index< filenames.length; index++) {
			attachementNames[index] = filePath + sdf.format(new Date())+"_"+filenames[index];
		}
		return attachementNames;
	}
	
	private void clearFiles( String[] attachmentsNames) {
		for(String fileName : attachmentsNames) {
			File file = new File(fileName);
			if(file.exists())
				file.delete();
		}
	}


	public void sendMail() { 

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

		String[] attachmentsNames = transFilePathName(attachmentFileNames);
		createFile(attachmentsNames[0], tableBuffer.toString());
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
			tableBuffer.append(bigLength.count_10k).append("    |    ");
		}

		//
		try {

			createFile(attachmentsNames[1], tableBuffer.toString());

			StringBuffer body = new StringBuffer();
			body.append("");
			body.append("");
			body.append("");
			body.append("");

			MailUtil.send(" ## RedisProxy Report ##", body.toString(), attachmentsNames);
		} finally {
			clearFiles(attachmentsNames);
		}

	}

}
