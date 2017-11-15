package com.feeyo.util;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class MailUtil {
	
	private static Properties props = null;
	
	private static String userName = "";
	private static String password = "";
	
	private static InternetAddress fromAddr;
	
	static {
		 props = new Properties();
		 props.setProperty("mail.transport.protocol", "smtp");
		 props.setProperty("mail.smtp.host", "smtp.exmail.qq.com");
		 props.setProperty("mail.smtp.port", "25");
		 props.setProperty("mail.smtp.auth", "true");
		 
		 userName =  "monitor@feeyo.com";
		 password = "CTv-LIvhV%1";
		 
		 try {
			fromAddr = new InternetAddress( userName );
		} catch (AddressException e) {
			e.printStackTrace();
		}
	}
	
	public static void send(String[] addrs, String subject, String body) {  
		
		
		
        
        //建立邮件会话
        Session session = Session.getDefaultInstance(props, new Authenticator() {
            //身份认证
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, password);
            }
        });
        //session.setDebug( true );
        
		try {
			
			InternetAddress[] toAddrs = new InternetAddress[ addrs.length ];
			for(int i =0; i < addrs.length; i++) {
				toAddrs[i]= new InternetAddress( addrs[i] );
			}
			
			MimeMessage message = new MimeMessage(session);
	        message.setFrom( fromAddr );
	        message.setRecipients(MimeMessage.RecipientType.TO, toAddrs);
	        message.setSubject(subject, "UTF-8");
	        message.setContent(body, "text/html; charset=UTF-8");
	        message.setSentDate(new Date());
	        message.saveChanges();
			
	        Transport.send(message);

		} catch (MessagingException e) {
			e.printStackTrace();
		}
	}
	
}