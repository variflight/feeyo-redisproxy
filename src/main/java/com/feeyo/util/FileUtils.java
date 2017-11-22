package com.feeyo.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
	private static Logger LOGGER = LoggerFactory.getLogger( FileUtils.class );
	private static ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(); 
	
	public static boolean ensureCreateFile(String filename, String content, boolean isAppend) {
		FileOutputStream fos = null;
		OutputStreamWriter out = null;
		BufferedWriter bw = null;
		try {
			rwLock.writeLock().lock();
			File file =new File(filename);
			
			if(!file.getParentFile().exists()) {
				file.getParentFile().mkdir();
			}
	        if(!file.exists()){  
	        	file.createNewFile();
	        	LOGGER.info("Create file {}", filename);
	        }
			fos = new FileOutputStream(file,isAppend);
			out =new OutputStreamWriter(fos,"utf-8");
	        bw = new BufferedWriter(out);
	        bw.write(content);
	        bw.flush();
	        return true;
		} catch (IOException e) {
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
			rwLock.writeLock().unlock();
		}
		return false;
	}
	
	//使用Scanner，避免大文件全部存储进内存。
	public static List<String> readFile(String filename) {
		//避免lines为null的情况
		List<String>  lines = new ArrayList<String>();
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			rwLock.readLock().lock();
		    inputStream = new FileInputStream(filename);
		    sc = new Scanner(inputStream, "UTF-8");
		    while (sc.hasNextLine()) {
		        lines.add(sc.nextLine());
		    }
		} catch (FileNotFoundException e) {
		} finally {
		    if (inputStream != null) {
		        try {
					inputStream.close();
				} catch (IOException e) {
				}
		    }
		    if (sc != null) {
		        sc.close();
		    }
		    rwLock.readLock().unlock();
		}
		return lines;
	}
	
	public static void cleanFiles( String[] filenames) {
		for(String filename : filenames) {
			File file = new File(filename);
			if(file.exists()) {
				file.delete();
				LOGGER.info("Clean the file : {}",filename);
			}
		}
	}
	
	public static void cleanFile(String filename) {
		File file = new File(filename);
		if(file.exists()) {
			file.delete();
			LOGGER.info("Clean the file : {}",filename);
		}
	}
	
	public static String[] getFilenamesUnderDir(String dirPath) {
		List<String> filenames = new ArrayList<String>();
		File file = new File(dirPath);
        if (file.exists()) {
            File[] files = file.listFiles();
            for(File file2 : files)
            	if(file2.isFile())
            		filenames.add(file2.getPath());
        }
		return filenames.toArray(new String[0]);
	}
	
}
