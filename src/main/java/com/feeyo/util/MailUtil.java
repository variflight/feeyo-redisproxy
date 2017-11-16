package com.feeyo.util;

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
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

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
	
	public static boolean send(String[] addrs, String subject, String body,String[] fileNames) {

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