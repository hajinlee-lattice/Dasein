package com.latticeengines.security.util;

import java.util.Collection;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.EmailSettings;

public final class EmailUtils {
    private static final Log log = LogFactory.getLog(EmailUtils.class);

    private EmailUtils() {
    }

    public static void sendSimpleEmail(String subject, Object content, String contentType,
            Collection<String> recipients, EmailSettings emailSettings) {
        try {
            Message message = new MimeMessage(applySettings(emailSettings));
            message.setFrom(new InternetAddress(emailSettings.getFrom()));
            for (String recipient : recipients) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            }
            message.setSubject(subject);
            message.setContent(content, contentType);

            Transport.send(message);
        } catch (MessagingException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a simple email", e);
        }
    }

    public static void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
            EmailSettings emailSettings) {
        try {
            log.info("Begining to send multi part email now.");
            Message message = new MimeMessage(applySettings(emailSettings));
            message.setFrom(new InternetAddress(emailSettings.getFrom()));
            for (String recipient : recipients) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            }
            message.setSubject(subject);
            message.setContent(content);

            log.info("Begining to send multi part email before calling transport.");
            Transport.send(message);
        } catch (MessagingException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a multipart email", e);
        }
    }

    public static Session applySettings(final EmailSettings emailSettings) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.smtp.starttls.enable", emailSettings.isUseSTARTTLS());
        props.put("mail.smtp.host", emailSettings.getServer());
        props.put("mail.smtp.port", emailSettings.getPort());

        if (emailSettings.isUseSSL()) {
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        }

        if (emailSettings.getPort() == 25) {
            props.put("mail.smtp.auth.plain.disable", true);
        } else {
            props.put("mail.smtp.auth", "true");
        }

        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(emailSettings.getUsername(), emailSettings.getPassword());
            }
        });

        return session;
    }

}
