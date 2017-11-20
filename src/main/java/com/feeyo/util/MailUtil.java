package com.feeyo.util;

import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Authenticator;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class MailUtil {
	
	private static Properties props = null;
	private static String userName = null;
	private static String password = null;
	
	private static InternetAddress fromAddr;
	
	private static Object _lock = new Object();
	
	
	public static boolean send(String[] addrs, String subject, String body,String[] fileNames) {
		
		//
		if (props == null) {
			synchronized ( _lock ) {	
				if ( props == null ) {
					try {
						props = new Properties();
						props.load(new FileInputStream(System.getProperty("FEEYO_HOME") + "/conf/mail.properties"));
						
						userName = props.getProperty("mail.from.userName");
						password = props.getProperty("mail.from.password");
						fromAddr = new InternetAddress(userName);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				
			}
		}

	 	Session session = Session.getDefaultInstance(props, new Authenticator() {
            //身份认证
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, password);
            }
        });
	 	
        try {
        	
        	InternetAddress[] toAddrs = new InternetAddress[ addrs.length ];
			for(int i =0; i < addrs.length; i++) {
				toAddrs[i]= new InternetAddress( addrs[i] );
			}
			
			MimeMessage message = new MimeMessage(session);
	        message.setFrom( fromAddr );
	        message.setRecipients(MimeMessage.RecipientType.TO, toAddrs);
			
	        message.setSubject(subject, "UTF-8");
	        Multipart mp = new MimeMultipart();  
            // 附件操作  
            if (fileNames.length > 0) {  
                for (int i=0;i < fileNames.length; i++) {  
                    MimeBodyPart mbp = new MimeBodyPart();  
                    // 得到数据源  
                    FileDataSource fds = new FileDataSource(fileNames[i]);  
                    // 得到附件本身并至入BodyPart  
                    mbp.setDataHandler(new DataHandler(fds));  
                    // 得到文件名同样至入BodyPart  
                    mbp.setFileName(fds.getName());  
                    mp.addBodyPart(mbp);  
                }  
                MimeBodyPart mbp = new MimeBodyPart();  
                mbp.setText(body);  
                mp.addBodyPart(mbp);  
                // Multipart加入到信件  
                message.setContent(mp);  
            }else {
                // 设置邮件正文  
                message.setText(body);  
            }
            message.setSentDate(new Date());
	        message.saveChanges();
	        
            // 发送邮件
            Transport.send(message);

        } catch (MessagingException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
	 
}