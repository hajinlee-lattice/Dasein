package com.latticeengines.dellebi.util;

import java.util.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

public class MailSender {

    private final static Logger LOGGER = Logger.getLogger(MailSender.class);

    @Value("${dellebi.mailhost}")
    private String mailHost;
    @Value("${dellebi.mailfrom}")
    private String mailFrom;

    public Boolean sendEmail(String aToEmailAddr, String aSubject, String aBody) {

        Properties fMailServerConfig = new Properties();
        fMailServerConfig.setProperty("mail.smtp.host", mailHost);
        Session session = Session.getDefaultInstance(fMailServerConfig, null);
        MimeMessage message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress(mailFrom));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(aToEmailAddr));
            message.setSubject(aSubject);
            message.setText(aBody);
            Transport.send(message);
            return true;
        } catch (MessagingException ex) {
            LOGGER.warn("Cannot send email. " + ex);
            return false;
        }
    }

    public void setMailHost(String mailHost) {
        this.mailHost = mailHost;
    }

    public void setmailFrom(String mailFrom) {
        this.mailFrom = mailFrom;
    }

}
