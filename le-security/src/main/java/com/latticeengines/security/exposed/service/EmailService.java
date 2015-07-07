package com.latticeengines.security.exposed.service;

import java.util.Collection;

import javax.mail.Multipart;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public interface EmailService {

    void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients);

    void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients);

    void sendPLSNewInternalUserEmail(Tenant tenant, User user, String password, String hostport);

    void sendPLSNewExternalUserEmail(User user, String password, String hostport);

    void sendPLSExistingInternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPLSExistingExternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPLSForgetPasswordEmail(User user, String password, String hostport);
}
