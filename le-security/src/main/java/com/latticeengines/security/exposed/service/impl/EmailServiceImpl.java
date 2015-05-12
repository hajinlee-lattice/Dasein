package com.latticeengines.security.exposed.service.impl;

import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
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
            Email email = new SimpleEmail();
            applySettings(email, emailSettings);
            email.setSubject(subject);
            email.setContent(content, contentType);
            for (String recipient : recipients) {
                email.addTo(recipient);
            }
            email.send();
        } catch (EmailException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a simple email", e);
        }
    }

    private void applySettings(Email email, EmailSettings emailSettings) throws EmailException {
        email.setHostName(emailSettings.getServer());
        email.setSmtpPort(emailSettings.getPort());
        email.setSSLOnConnect(emailSettings.isUseSSL());
        email.setStartTLSEnabled(emailSettings.isUseSTARTTLS());
        email.setFrom(emailSettings.getFrom());
    }
}
