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

    void sendPdNewExternalUserEmail(User user, String password, String hostport);

    void sendPdExistingExternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPlsForgetPasswordEmail(User user, String password, String hostport);

    void sendPlsImportDataSuccessEmail(User user, String hostport);

    void sendPlsImportDataErrorEmail(User user, String hostport);

    void sendPlsEnrichDataSuccessEmail(User user, String hostport);

    void sendPlsEnrichDataErrorEmail(User user, String hostport);

    void sendPlsValidateMetadataSuccessEmail(User user, String hostport);

    void sendPlsMetadataMissingEmail(User user, String hostport);

    void sendPlsValidateMetadataErrorEmail(User user, String hostport);

    void sendPlsCreateModelCompletionEmail(User user, String hostport);

    void sendPlsCreateModelErrorEmail(User user, String hostport);

    void sendPlsScoreCompletionEmail(User user, String hostport);

    void sendPlsScoreErrorEmail(User user, String hostport);
}
