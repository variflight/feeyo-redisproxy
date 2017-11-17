package com.feeyo.redis.engine.mail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MailProperty {
	private String username = null;
	private String password = null;
	private String[] addrs = null;
	private String protocol = null;
	private String host = null;
	private int port;
	private boolean isAuth;
	private Properties property = null;
	
	public static MailProperty updateProperty() {
		Properties property = new Properties();
		MailProperty mailProperty = new MailProperty();
		FileInputStream fis = null;
		InputStream in = null;
		try {
			fis = new FileInputStream(System.getProperty("FEEYO_HOME")+File.separator+"mail.properties");
			in = new BufferedInputStream(fis);
			if(in != null) {
				property.load(in);
				mailProperty.username = property.getProperty("fromUser");
				mailProperty.password = property.getProperty("fromUserPwd");
				mailProperty.addrs = transformAddrs(property.getProperty("toUser"));
				mailProperty.protocol = property.getProperty("mail.transport.protocol");
				mailProperty.host = property.getProperty("mail.smtp.host");
				mailProperty.port = Integer.parseInt(property.getProperty("mail.smtp.port"));
				mailProperty.isAuth = "true".equalsIgnoreCase(property.getProperty("mail.smtp.auth"))? true : false;
				mailProperty.property = property;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if( in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
		return mailProperty;
	}
	private static String[] transformAddrs(String property) {
		return property.split(";");
	}
	public String getUsername() {
		return username;
	}
	public String getPassword() {
		return password;
	}
	public String[] getAddrs() {
		return addrs;
	}
	public String getProtocol() {
		return protocol;
	}
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	public boolean isAuth() {
		return isAuth;
	}
	public Properties getProperty() {
		return property;
	}
}