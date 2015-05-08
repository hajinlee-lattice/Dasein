package com.latticeengines.security.exposed.service.impl;

import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.commons.mail.ImageHtmlEmail;
import org.apache.commons.mail.SimpleEmail;
import org.apache.commons.mail.resolver.DataSourceClassPathResolver;
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

    @PostConstruct
    private void setupDefaultEmailSettings() {
        emailsettings = new EmailSettings();
        emailsettings.setFrom(EMAIL_FROM);
        emailsettings.setServer(EMAIL_SERVER);
        emailsettings.setUsername(EMAIL_USERNAME);
        emailsettings.setPassword(EMAIL_PASSWORD);
        emailsettings.setPort(EMAIL_PORT);
        emailsettings.setUseSSL(EMAIL_USESSL);
    }

    @Override
    public void sendSimpleEmail(String subject, String content, Collection<String> recipients) {
        sendSimpleEmail(subject, content, recipients, emailsettings);
    }

    @Override
    public void sendSimpleEmail(String subject, String content, Collection<String> recipients,
                                EmailSettings emailSettings) {
        try {
            Email email = new SimpleEmail();
            email.setHostName(emailSettings.getServer());
            email.setSmtpPort(emailSettings.getPort());
            email.setAuthenticator(new DefaultAuthenticator(emailSettings.getUsername(), emailSettings.getPassword()));
            email.setSSLOnConnect(emailSettings.isUseSSL());
            email.setFrom(emailSettings.getFrom());
            email.setSubject(subject);
            email.setMsg(content);
            for (String recipient : recipients) {
                email.addTo(recipient);
            }
            email.send();
        } catch (EmailException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending a simple email", e);
        }
    }

    @Override
    public void sendHtmlEmail(String subject, String htmlTemplate, Collection<String> recipients) {
        String alternativeMsg = "Your email client does not support HTML messages";
        sendHtmlEmail(subject, htmlTemplate,alternativeMsg, recipients, emailsettings);
    }

    @Override
    public void sendHtmlEmail(String subject, String htmlTemplate,String alternativeMsg,
                              Collection<String> recipients) {
        sendHtmlEmail(subject, htmlTemplate,alternativeMsg, recipients, emailsettings);
    }

    @Override
    public void sendHtmlEmail(String subject, String htmlTemplate,
                              Collection<String> recipients, EmailSettings emailSettings) {
        String alternativeMsg = "Your email client does not support HTML messages";
        sendHtmlEmail(subject, htmlTemplate, alternativeMsg, recipients, emailSettings);

    }

    @Override
    public void sendHtmlEmail(String subject, String htmlTemplate,String alternativeMsg,
                              Collection<String> recipients, EmailSettings emailSettings) {

        try {
            ImageHtmlEmail email = new ImageHtmlEmail();
            email.setDataSourceResolver(new DataSourceClassPathResolver("/"));
            email.setSubject(subject);
            for (String recipient : recipients) {
                email.addTo(recipient);
            }
            email.setHtmlMsg(htmlTemplate);
            email.setTextMsg(alternativeMsg);

            email.setHostName(emailSettings.getServer());
            email.setSmtpPort(emailSettings.getPort());
            email.setAuthenticator(new DefaultAuthenticator(emailSettings.getUsername(), emailSettings.getPassword()));
            email.setSSLOnConnect(emailSettings.isUseSSL());
            email.setFrom(emailSettings.getFrom());

            email.send();
        } catch (EmailException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending an html email", e);
        }
    }

    @Override
    public void sendHtmlEmail(HtmlEmail htmlEmail, EmailSettings emailSettings) {
        try {
            htmlEmail.setHostName(emailSettings.getServer());
            htmlEmail.setSmtpPort(emailSettings.getPort());
            htmlEmail.setAuthenticator(new DefaultAuthenticator(emailSettings.getUsername(), emailSettings.getPassword()));
            htmlEmail.setSSLOnConnect(emailSettings.isUseSSL());
            htmlEmail.setFrom(emailSettings.getFrom());
            htmlEmail.send();
        } catch (EmailException e) {
            throw new LedpException(LedpCode.LEDP_19000, "Error sending an html email", e);
        }

    }
}
