package com.latticeengines.security.exposed.service;

import java.util.Collection;

public interface EmailService {

    void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients);
}
