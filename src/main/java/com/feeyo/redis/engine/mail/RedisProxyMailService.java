package com.feeyo.redis.engine.mail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.mail.internet.AddressException;

import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.engine.manage.stat.StatUtil.BigKey;
import com.feeyo.redis.engine.manage.stat.StatUtil.CollectionKey;
import com.feeyo.redis.net.front.handler.CommandParse;

public class RedisProxyMailService {
	
	private static long twentyThreeMillis = System.currentTimeMillis();
	
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
	
	private static final String[] fileNames = new String[] {"big_key.txt", "top_hundred.txt"};
	private static final String filePath = System.getProperty("FEEYO_HOME")+File.separator+"store"+File.separator;
	
	public void startUp() {
		
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				
				if(System.currentTimeMillis() > twentyThreeMillis) {
					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.DATE, 1);
			        cal.set(Calendar.HOUR_OF_DAY, 22);
			        cal.set(Calendar.MINUTE, 59);
					cal.set(Calendar.SECOND, 59); 
					cal.set(Calendar.MILLISECOND, 59); 	
					
					ConcurrentHashMap<String, BigKey> bigkeyStats = StatUtil.getBigKeyStats();
					StringBuffer tableBuffer = new StringBuffer();
					String[] attachmentsNames = new String[fileNames.length];
					Collection<Entry<String, BigKey>> entrys = bigkeyStats.entrySet();
					if(!entrys.isEmpty()) {
            			tableBuffer.append("#############   bigkey status   #################\n");
            			tableBuffer.append("|    cmd    |     key     |    size    |    count    |");
            			//获取bigkey的数据信息
    					for(Entry<String, BigKey> entry : entrys) {
    						BigKey bigkey = entry.getValue();
    						tableBuffer.append("\n");
    						tableBuffer.append("|    ");
    						tableBuffer.append(bigkey.cmd).append("    |    ");
    						tableBuffer.append(bigkey.key).append("    |    ");
    						tableBuffer.append(bigkey.size).append("    |    ");
    						tableBuffer.append(bigkey.count).append("    |");
    					}
    					attachmentsNames = transFilePathName(attachmentsNames,0);
    					createFile(attachmentsNames[0],tableBuffer.toString());
    					tableBuffer.setLength(0);
					}
					
					Set<Entry<String, CollectionKey>> collectionKeySet =  StatUtil.getCollectionKeyTop100OfLength();
					if(!collectionKeySet.isEmpty()) {
            			tableBuffer.append("#############   top  hundred   #################\n");
            			tableBuffer.append("|    key    |     type     |    length    |    count_1k    |    count_10k    |");
						for(Entry<String, CollectionKey> entry : collectionKeySet) {
							CollectionKey collectionKey = entry.getValue();
							tableBuffer.append("\n");
							tableBuffer.append("|    ");
    						tableBuffer.append(collectionKey.key).append("    |    ");
    						String type = null;
    						if (collectionKey.type == CommandParse.TYPE_HASH_CMD) {
    							type = "hash";
    						} else if (collectionKey.type == CommandParse.TYPE_LIST_CMD) {
    							type = "list";
    						} else if (collectionKey.type == CommandParse.TYPE_SET_CMD) {
    							type = "set";
    						} else if (collectionKey.type == CommandParse.TYPE_SORTEDSET_CMD) {
    							type = "sortedset";
    						}
    						tableBuffer.append(type).append("    |    ");
    						tableBuffer.append(collectionKey.length).append("    |    ");
    						tableBuffer.append(collectionKey.count_1k).append("    |    ");
    						tableBuffer.append(collectionKey.count_10k).append("    |    ");
						}
						attachmentsNames = transFilePathName(attachmentsNames,1);
						createFile(attachmentsNames[1],tableBuffer.toString());
					}
					
					String[]  names = trimAttachement(attachmentsNames);
					if(names.length > 0) {
						MailClient mailClient = null;
						try {
							mailClient = new MailClient();
						}catch (IOException | AddressException e) {
						}
						if(null != mailClient) {
							String[] addrs = mailClient.getProperty().getProperty("mail.to.addrs").split(";");
							mailClient.send(addrs, " ## REDIS PROXY STATUS ##", "Please check the attachments", names);
							clearFiles(names);
						}
					}
					twentyThreeMillis = cal.getTimeInMillis();
				}
			}
		}, 10 * 60, 5 * 60, TimeUnit.SECONDS);
	}
	
	private String[] trimAttachement(String[] attachmentsNames) {
		List<String> list = new ArrayList<String>();
		for(String attachmentsName : attachmentsNames) {
			if(null != attachmentsName && !attachmentsName.trim().isEmpty())
				list.add(attachmentsName);
		}
		return list.toArray(new String[0]);
	}

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
		}finally {
			if(bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
				}
			}
			if(out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	private String[] transFilePathName(String[] attachementNames, int index) {
		attachementNames[index] = filePath + sdf.format(new Date())+"_"+fileNames[index];
		return attachementNames;
	}
	
	private void clearFiles( String[] attachmentsNames) {
		for(String fileName : attachmentsNames) {
			File file = new File(fileName);
			if(file.exists())
				file.delete();
		}
	}
}
