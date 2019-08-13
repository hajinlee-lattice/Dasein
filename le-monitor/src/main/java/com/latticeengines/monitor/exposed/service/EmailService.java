package com.latticeengines.monitor.exposed.service;

import java.util.Collection;
import java.util.List;

import javax.mail.Multipart;

import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.monitor.EmailSettings;
import com.latticeengines.domain.exposed.pls.CancelActionEmailInfo;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public interface EmailService {

    void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients);

    void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients);

    void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
                            Collection<String> bccRecipients);

    void sendPlsNewInternalUserEmail(Tenant tenant, User user, String password, String hostport);

    void sendPlsNewProspectingUserEmail(User user, String password, String hostport);

    void sendPlsNewExternalUserEmail(User user, String password, String hostport, boolean bccEmail);

    void sendPlsExistingInternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPlsExistingExternalUserEmail(Tenant tenant, User user, String hostport, boolean bccEmail);

    void sendPdNewExternalUserEmail(User user, String password, String hostport);

    void sendPdExistingExternalUserEmail(Tenant tenant, User user, String hostport);

    void sendPlsForgetPasswordEmail(User user, String password, String hostport);

    void sendPlsForgetPasswordConfirmationEmail(String userEmail, String hostport);

    void sendPlsImportDataSuccessEmail(User user, String hostport);

    void sendPlsImportDataErrorEmail(User user, String hostport);

    void sendPlsEnrichDataSuccessEmail(User user, String hostport);

    void sendPlsEnrichDataErrorEmail(User user, String hostport);

    void sendPlsValidateMetadataSuccessEmail(User user, String hostport);

    void sendPlsMetadataMissingEmail(User user, String hostport);

    void sendPlsValidateMetadataErrorEmail(User user, String hostport);

    void sendPlsCreateModelCompletionEmail(User user, String hostport, String tenantName, String modelName,
                                           boolean internal);

    void sendPlsCreateModelErrorEmail(User user, String hostport, String tenantName, String modelName,
                                      boolean internal);

    void sendPlsScoreCompletionEmail(User user, String hostport, String tenantName, String modelName, boolean internal);

    void sendPlsEnrichInternalAttributeErrorEmail(User user, String hostport, String tenantName, String modelName,
                                                  boolean internal, List<String> internalAttributes);

    void sendPlsEnrichInternalAttributeCompletionEmail(User user, String hostport, String tenantName, String modelName,
                                                       boolean internal, List<String> internalAttributes);

    void sendPlsScoreErrorEmail(User user, String hostport, String tenantName, String modelName, boolean internal);

    void sendPlsOnetimeSfdcAccessTokenEmail(User user, String tenantId, String accessToken);

    void sendGlobalAuthForgetCredsEmail(String firstName, String lastName, String username, String password,
                                        String emailAddress, EmailSettings settings);

    void sendPlsExportSegmentSuccessEmail(User user, String hostport, String exportID, String exportType);

    void sendPlsExportSegmentErrorEmail(User user, String exportID, String type);

    void sendPlsExportSegmentRunningEmail(User user, String exportID);

    void sendPlsExportOrphanRecordsRunningEmail(User user, String exportID, String type);

    void sendPlsExportOrphanRecordsSuccessEmail(User user, String tenantName, String hostport, String url,
                                                String exportID, String type);

    void sendCDLProcessAnalyzeCompletionEmail(User user, Tenant tenant, String appPublicUrl);

    void sendCDLProcessAnalyzeErrorEmail(User user, Tenant tenant, String appPublicUrl);

    void sendPOCTenantStateNoticeEmail(User user, Tenant tenant, String state, int days);

    void sendTenantRightStatusNoticeEmail(User user, Tenant tenant, int days);

    void sendS3CredentialEmail(User user, Tenant tenant, GrantDropBoxAccessResponse response, String initiator);

    void sendIngestionStatusEmail(User user, Tenant tenant, String hostport, String status, S3ImportEmailInfo emailInfo);

    void sendS3TemplateCreateEmail(User user, Tenant tenant, String hostport, S3ImportEmailInfo emailInfo);

    void sendS3TemplateUpdateEmail(User user, Tenant tenant, String hostport, S3ImportEmailInfo emailInfo);

    void sendPlsActionCancelSuccessEmail(User user, String hostport, CancelActionEmailInfo cancelActionEmailInfo);
}
