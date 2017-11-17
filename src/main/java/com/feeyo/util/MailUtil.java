package com.feeyo.util;

import java.util.Date;

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
import com.feeyo.redis.engine.mail.MailProperty;

public class MailUtil {
	
	public static boolean send(final MailProperty mailProperty, String subject, String body,String[] fileNames) {

	 	Session session = Session.getDefaultInstance(mailProperty.getProperty(), new Authenticator() {
            //身份认证
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(mailProperty.getUsername(), mailProperty.getPassword());
            }
        });
	 	
        try {
        	
        	InternetAddress[] toAddrs = new InternetAddress[ mailProperty.getAddrs().length ];
			for(int i =0; i < mailProperty.getAddrs().length; i++) {
				toAddrs[i]= new InternetAddress( mailProperty.getAddrs()[i] );
			}
			
			MimeMessage message = new MimeMessage(session);
			InternetAddress fromAddr = new InternetAddress( mailProperty.getUsername() );
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