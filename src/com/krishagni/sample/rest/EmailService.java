package com.krishagni.sample.rest;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EmailService {
	
	public static String from;
	
	public static String to;
	
	public static String username;
	
	public static String password;
	
	private static String host = "smtp.gmail.com";
	
	private static String port = "587";
	
	private Session session;
	
	private void authenticate() {
		Properties props = new Properties();
	    props.put("mail.smtp.auth", "true");
	    props.put("mail.smtp.starttls.enable", "true");
	    props.put("mail.smtp.host", host);
	    props.put("mail.smtp.port", port);
	    
	    session = Session.getInstance(props,
    		new javax.mail.Authenticator() {
               protected PasswordAuthentication getPasswordAuthentication() {
                  return new PasswordAuthentication(username, password);
               }
            }
	    );
	}
	
	public void sendMail(String status, String objectType) {
		authenticate();
		String subject = objectType + " bulk-import got " + status + ". <EOM>";
		
		try {
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(from));
			message.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
			message.setSubject(subject);
			message.setContent("", "text/plain");
			
			Transport.send(message);
		} catch(MessagingException ex) {
			ex.printStackTrace();
		}
	}
}