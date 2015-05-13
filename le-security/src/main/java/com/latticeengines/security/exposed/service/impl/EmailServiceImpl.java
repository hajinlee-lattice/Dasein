package com.latticeengines.security.exposed.service.impl;

import java.util.Collection;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.EmailSettings;
import com.latticeengines.security.exposed.service.EmailService;

@Component
public class EmailServiceImpl implements EmailService {

    private static EmailSettings emailsettings;

    @Value("${security.emailsettings.from}")
    private String EMAIL_FROM;

    @Value("${security.emailsettings.server}")
    private String EMAIL_SERVER;

    @Value("${security.emailsettings.username}")
    private String EMAIL_USERNAME;

    @Value("${security.emailsettings.password}")
    private String EMAIL_PASSWORD;

    @Value("${security.emailsettings.port}")
    private int EMAIL_PORT;

    @Value("${security.emailsettings.useSSL}")
    private boolean EMAIL_USESSL;

    @Value("${security.emailsettings.useSTARTTLS}")
    private boolean EMAIL_USESTARTTLS;

    @PostConstruct
    private void setupDefaultEmailSettings() {
        emailsettings = new EmailSettings();
        emailsettings.setFrom(EMAIL_FROM);
        emailsettings.setServer(EMAIL_SERVER);
        emailsettings.setUsername(EMAIL_USERNAME);
        emailsettings.setPassword(EMAIL_PASSWORD);
        emailsettings.setPort(EMAIL_PORT);
        emailsettings.setUseSSL(EMAIL_USESSL);
        emailsettings.setUseSTARTTLS(EMAIL_USESTARTTLS);
    }

    @Override
    public void sendSimpleEmail(String subject, Object content, String contentType,
                                     Collection<String> recipients) {
        sendSimpleEmail(subject, content, contentType, recipients, emailsettings);
    }

    public void sendSimpleEmail(String subject, Object content, String contentType,
                                Collection<String> recipients, EmailSettings emailSettings) {
        try {
            Message message = new MimeMessage(applySettings(emailSettings));
            message.setFrom(new InternetAddress(emailSettings.getFrom()));
            for (String recipient: recipients) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            }
            message.setSubject(subject);
            message.setContent(content, contentType);

            Transport.send(message);
        } catch (MessagingException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a simple email", e);
        }
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients) {
        sendMultiPartEmail(subject, content, recipients, emailsettings);
    }

    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
                                  EmailSettings emailSettings) {
        try {
            Message message = new MimeMessage(applySettings(emailSettings));
            message.setFrom(new InternetAddress(emailSettings.getFrom()));
            for (String recipient: recipients) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            }
            message.setSubject(subject);
            message.setContent(content);

            Transport.send(message);
        } catch (MessagingException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a simple email", e);
        }
    }

    public Session applySettings(final EmailSettings emailSettings) throws MessagingException {
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

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(emailSettings.getUsername(), emailSettings.getPassword());
                    }
                });

        return session;
    }
}
