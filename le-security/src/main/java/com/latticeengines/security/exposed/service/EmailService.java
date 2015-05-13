package com.latticeengines.security.exposed.service;

import java.util.Collection;

import javax.mail.Multipart;

public interface EmailService {

    void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients);

    void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients);
}
