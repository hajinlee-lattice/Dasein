package com.latticeengines.security.exposed.service;

import java.util.Collection;

import org.apache.commons.mail.HtmlEmail;

import com.latticeengines.domain.exposed.security.EmailSettings;

public interface EmailService {

    void sendSimpleEmail(String subject, String content, Collection<String> recipients, EmailSettings emailSettings);

    void sendSimpleEmail(String subject, String content, Collection<String> recipients);

    void sendHtmlEmail(HtmlEmail htmlEmail, EmailSettings emailSettings);

    void sendHtmlEmail(String subject, String htmlTemplate, Collection<String> recipients);

    void sendHtmlEmail(String subject, String htmlTemplate, String alternativeMsg, Collection<String> recipients);

    void sendHtmlEmail(String subject, String htmlTemplate, Collection<String> recipients, EmailSettings emailSettings);

    void sendHtmlEmail(String subject, String htmlTemplate, String alternativeMsg,
                       Collection<String> recipients, EmailSettings emailSettings);
}
