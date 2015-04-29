package com.latticeengines.dellebi.util;

import java.util.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dellebi.flowdef.DailyFlow;

public class MailSender {

	private static final Log log = LogFactory.getLog(MailSender.class);

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
            log.warn("Cannot send email. " + ex);
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
