package com.latticeengines.security.exposed.service;

import java.util.Collection;

import javax.mail.Multipart;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public interface EmailService {

    void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients);

    void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients);

    void sendPlsNewInternalUserEmail(Tenant tenant, User user, String password, String hostport);

    void sendPlsNewExternalUserEmail(User user, String password, String hostport);

    void sendPlsExistingInternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPlsExistingExternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPlsForgetPasswordEmail(User user, String password, String hostport);
}
