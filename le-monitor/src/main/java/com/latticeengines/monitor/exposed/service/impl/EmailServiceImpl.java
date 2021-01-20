package com.latticeengines.monitor.exposed.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.datacloud.manage.DateTimeUtils;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.monitor.EmailSettings;
import com.latticeengines.domain.exposed.pls.CancelActionEmailInfo;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.util.EmailTemplateBuilder;
import com.latticeengines.monitor.util.EmailTemplateBuilder.Template;
import com.latticeengines.monitor.util.EmailUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Component
public class EmailServiceImpl implements EmailService {

    // need to manipulate this log in EmailServiceImplTestNG
    @SuppressFBWarnings({"SLF4J_LOGGER_SHOULD_BE_PRIVATE", "SLF4J_LOGGER_SHOULD_BE_FINAL"})
    protected static Logger log = LoggerFactory.getLogger(EmailServiceImpl.class);

    private static final String COMMA = ", ";

    @Inject
    @Qualifier("emailSettings")
    private EmailSettings smtpEmailSettings;

    @Inject
    @Qualifier("sendgridEmailSettings")
    private EmailSettings sendgridEmailSettings;

    @Inject
    @Qualifier("dnbEmailSettings")
    private EmailSettings dnbEmailSettings;

    @Inject
    @Qualifier("sendgridLatticeEmailSettings")
    private EmailSettings sendgridLatticeEmailSettings;

    @Value("${monitor.use.sendgrid}")
    private boolean useSendGrid;

    @Value("${monitor.use.dnb}")
    private boolean useDnb;

    @Value("${monitor.email.enabled:true}")
    private boolean emailEnabled;

    @Value("${monitor.email.businessops:LatticePODs@dnb.com}")
    private String businessOpsEmail;

    @Value("${monitor.email.pm:dev@lattice-engines.com}")
    private String pmEmail;

    @Value("${monitor.urls.helpcenter}")
    private String helpCenterUrl;

    @Value("${common.dcp.public.url}")
    private String dcpPublicUrl;

    @VisibleForTesting
    public void enableEmail() {
        emailEnabled = true;
    }

    /**
     * Method returns the EmailSetting object containing the setting for the From: address and
     * the email provider to use for that From: address.  The settings to use are based on the valud
     * of the fromAddress parameter.
     *
     * @return - EmailSettings object containing the return address and the email service configuration.
     */
    private EmailSettings getEmailSettings(EmailFromAddress fromAddress) {
        if (fromAddress == EmailFromAddress.DNB_CONNECT) {
            return useSendGrid ? sendgridEmailSettings : dnbEmailSettings;
        } else {
            return sendgridLatticeEmailSettings;
        }
    }

    /**
     * Send a 'simple' email that will contain a single part.  This method will use 'no-reply@connect.dnb.com' as
     * the From: address of the email message.
     * @param subject - Subject of the message
     * @param content - Content of the message
     * @param contentType - mime type of the content
     * @param recipients - Collection of recipients of the message
     */
    @Override
    public void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients) {
        sendSimpleEmail(subject, content, contentType, recipients, EmailFromAddress.DNB_CONNECT);
    }

    /**
     * Send a 'simple' email or single part email message.  This method takes a EmailFromAddress
     * parameter to specify the return address and email service configuration to use.
     * @param subject - Subject of the message
     * @param content - Content of the message
     * @param contentType - Content type of the message
     * @param recipients - Collection of recipients of the email
     * @param fromAddress - Enum that specifies the return address and the configuration for that address
     */
    @Override
    public void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients,
                                EmailFromAddress fromAddress) {
        if (emailEnabled) {
            EmailUtils.sendSimpleEmail(subject, content, contentType, recipients, getEmailSettings(fromAddress));
        }
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients) {
        sendMultiPartEmail(subject, content, recipients, EmailFromAddress.LATTICE_ENGINES);
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
                                   EmailFromAddress fromAddress) {
        if (emailEnabled) {
            EmailUtils.sendMultiPartEmail(subject, content, recipients, null,
                    getEmailSettings(fromAddress));
        }
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
                                   Collection<String> bccRecipients) {
        sendMultiPartEmail(subject, content, recipients, bccRecipients, EmailFromAddress.LATTICE_ENGINES);
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
                                   Collection<String> bccRecipients, EmailFromAddress fromAddress) {
        if (emailEnabled) {
            EmailUtils.sendMultiPartEmail(subject, content, recipients, bccRecipients, getEmailSettings(fromAddress));
        }
    }

    @Override
    public void sendNewUserEmail(User user, String password, String hostport, boolean bccEmail) {
        try {
            log.info("Sending new user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.NEW_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);
            builder.replaceToken("{{helpcenterurl}}", helpCenterUrl);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            if (bccEmail) {
                sendMultiPartEmail(EmailSettings.NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()),
                        Collections.singleton(businessOpsEmail));
            } else {
                sendMultiPartEmail(EmailSettings.NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()));
            }
            log.info("Sending new user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send new email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendExistingUserEmail(Tenant tenant, User user, String hostport, boolean bccEmail) {
        try {
            log.info("Sending existing user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.EXISTING_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            if (bccEmail) {
                sendMultiPartEmail(EmailSettings.EXISTING_USER_SUBJECT, mp,
                        Collections.singleton(user.getEmail()), Collections.singleton(businessOpsEmail));
            } else {
                sendMultiPartEmail(EmailSettings.EXISTING_USER_SUBJECT, mp,
                        Collections.singleton(user.getEmail()));
            }
            log.info("Sending existing user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send existing user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsForgetPasswordEmail(User user, String password, String hostport) {
        try {
            log.info("Sending forget password email " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_FORGET_PASSWORD);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_FORGET_PASSWORD_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending forget password email " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send forget password email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsForgetPasswordConfirmationEmail(User user, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_FORGET_PASSWORD_CONFIRMATION);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_FORGET_PASSWORD_CONFIRMATION_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending forget password email " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send forget password email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsCreateModelCompletionEmail(User user, String hostport, String tenantName, String modelName,
                                                  boolean internal) {
        try {
            log.info("Sending PLS create model (" + modelName + ") complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;

            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenantName);
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_CREATE_MODEL_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_JOB_SUCCESS_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model (" + modelName + ") complete email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsCreateModelErrorEmail(User user, String hostport, String tenantName, String modelName,
                                             boolean internal) {
        try {
            log.info("Sending PLS create model (" + modelName + ") error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenantName);
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_CREATE_MODEL_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_JOB_ERROR_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model (" + modelName + ") error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model (" + modelName + ") error email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsScoreCompletionEmail(User user, String hostport, String tenantName, String modelName,
                                            boolean internal) {
        try {
            log.info("Sending PLS scoring (" + modelName + ") complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;

            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenantName);
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_SCORE_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_JOB_SUCCESS_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create scoring (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring (" + modelName + ") complete email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsScoreErrorEmail(User user, String hostport, String tenantName, String modelName,
                                       boolean internal) {
        try {
            log.info("Sending PLS scoring (" + modelName + ") error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenantName);
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_SCORE_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_JOB_ERROR_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info(
                    "Sending PLS create scoring (" + modelName + ") error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring (" + modelName + ") error email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsOnetimeSfdcAccessTokenEmail(User user, String tenantId, String accessToken) {
        try {
            log.info("Sending PLS one-time sfdc access token email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_ONETIME_SFDC_ACCESS_TOKEN);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{username}}", tenantId);
            builder.replaceToken("{{accessToken}}", accessToken);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_ONE_TIME_SFDC_ACCESS_TOKEN_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info(String.format("Sending PLS one-time SFDC access token to: %s for tenant: %s succeeded",
                    user.getEmail(), tenantId));
        } catch (Exception e) {
            log.error(String.format("Sending PLS one-time SFDC access token to: %s for tenant: %s failed",
                    user.getEmail(), tenantId), e);
        }
    }

    @Override
    public void sendGlobalAuthForgetCredsEmail(String firstName, String lastName, String username, String password,
                                               String emailAddress, EmailSettings settings) {
        try {
            log.info("Sending global auth forget creds email to " + emailAddress + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.SECURITY_GLOBALAUTH_EMAIL_TEMPLATE);

            builder.replaceToken("{!FirstName}", firstName);
            builder.replaceToken("{!LastName}", lastName);
            builder.replaceToken("{!Username}", username);
            builder.replaceToken("{!Password}", password);

            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(builder, "text/html");
            mp.addBodyPart(htmlPart);
            EmailUtils.sendMultiPartEmail(EmailSettings.GLOBAL_AUTH_FORGET_CREDS_EMAIL_SUBJECT, mp,
                    Collections.singleton(emailAddress), null, settings);
        } catch (Exception e) {
            log.error(String.format("Sending global auth forget credentials email to :%s failed", emailAddress));
        }
    }

    @Override
    public void sendPlsEnrichInternalAttributeCompletionEmail(User user, String hostport, String tenantName,
                                                              String modelName, List<String> internalAttributes) {
        try {
            log.info("Sending PLS enrich internal attribute (" + modelName + ") complete email to " + user.getEmail()
                    + " started.");
            sendEmailForEnrichInternalAttribute(user, hostport, tenantName, modelName, internalAttributes,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS,
                    EmailSettings.PLS_INTERNAL_ATTRIBUTE_ENRICH_COMPLETION_EMAIL_SUBJECT);
            log.info("Sending PLS enrich internal attribute (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrich internal attribute (" + modelName + ") complete email to "
                    + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsEnrichInternalAttributeErrorEmail(User user, String hostport, String tenantName,
                                                         String modelName, List<String> internalAttributes) {
        try {
            log.info("Sending PLS enrich internal attribute (" + modelName + ") error email to " + user.getEmail()
                    + " started.");
            sendEmailForEnrichInternalAttribute(user, hostport, tenantName, modelName, internalAttributes,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR,
                    EmailSettings.PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_EMAIL_SUBJECT);
            log.info("Sending PLS enrich internal attribute (" + modelName + ") error email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrich internal attribute (" + modelName + ") error email to "
                    + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsExportSegmentSuccessEmail(User user, String hostport, String exportID, String exportType,
                                                 String tenantName) {
        try {
            log.info(String.format("Sending segment export complete email to %s started.", user.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_EXPORT_SEGMENT_SUCCESS);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{downloadLink}}", hostport);
            builder.replaceToken("{{exportID}}", exportID);
            builder.replaceToken("{{exportType}}", "segment");
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{requestType}}", exportType);
            builder.replaceToken("{{tenantName}}", tenantName);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/export-segment-instructions.png",
                    "image/png", "instruction");
            sendMultiPartEmail(EmailSettings.PLS_METADATA_SEGMENT_EXPORT_SUCCESS_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info(String.format("Sending PLS segment export complete email to %s succeeded.", user.getEmail()));
        } catch (Exception e) {
            log.error("Failed to send PLS segment export complete email to {}.", user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsExportSegmentErrorEmail(User user, String exportID, String type) {
        try {
            log.info("Sending PLS export segment error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_EXPORT_SEGMENT_ERROR);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{exportID}}", exportID);
            builder.replaceToken("{{requestType}}", type);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_METADATA_SEGMENT_EXPORT_ERROR_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS export segment error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS export segment error email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendPlsExportSegmentRunningEmail(User user, String exportID) {
        try {
            log.info(String.format("Sending PLS export segment in-progress email to %s started.", user.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_EXPORT_SEGMENT_RUNNING);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{requestType}}", "segment");
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_METADATA_SEGMENT_EXPORT_IN_PROGRESS_SUBJECT,
                    mp, Collections.singleton(user.getEmail()));
            log.info(String.format("Sending PLS export segment in-progress email to %s succeeded.", user.getEmail()));
        } catch (Exception e) {
            log.error(String.format("Failed to send PLS export segment in-progress email to %s. Exception message=%s",
                    user.getEmail(), e.getMessage()), e);
        }
    }

    @Override
    public void sendPlsAlwaysOnCampaignExpirationEmail(User creator, String launchInterval, String launchSettingsUrl,
            String playDisplayName,
            String externalSystemName, String nextScheduledLaunch) {
        try {
            log.info(
                    String.format("Sending PLS Always On expiration email to creator %s started.", creator.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_ALWAYS_ON_CAMPAIGN_EXPIRATION);
            builder.replaceToken("{{firstname}}", creator.getFirstName());
            builder.replaceToken("{{launchInterval}}", launchInterval);
            builder.replaceToken("{{launchSettingsUrl}}", launchSettingsUrl);
            builder.replaceToken("{{playDisplayName}}", playDisplayName);
            builder.replaceToken("{{externalSystemName}}", externalSystemName);
            builder.replaceToken("{{nextScheduledLaunch}}", nextScheduledLaunch);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_ALWAYS_ON_CAMPAIGN_EXPIRATION_SUBJECT),
                    mp, Collections.singleton(creator.getEmail()));
            log.info(String.format("Sending PLS Always On expiration email to %s succeeded.", creator.getEmail()));

        } catch (Exception e) {
            log.error(String.format("Failed to send PLS Always On expiration email to %s. Exception message=%s",
                    creator.getEmail(), e.getMessage()), e);
        }
    }

    @Override
    public void sendPlsCampaignFailedEmail(User creator, String launchHistoryUrl, String playDisplayName,
            String externalSystemName, String playLaunchState, String playLaunchCreated, String currentTime) {
        try {
            log.info(
                    String.format("Sending PLS playLaunch failed email to creator %s started.", creator.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_CAMPAIGN_FAILED);
            builder.replaceToken("{{firstname}}", creator.getFirstName());
            builder.replaceToken("{{launchHistoryLink}}", launchHistoryUrl);
            builder.replaceToken("{{playDisplayName}}", playDisplayName);
            builder.replaceToken("{{externalSystemName}}", externalSystemName);
            builder.replaceToken("{{playLaunchState}}", playLaunchState);
            builder.replaceToken("{{playLaunchCreated}}", playLaunchCreated);
            builder.replaceToken("{{currentTime}}", currentTime);
            
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_CAMPAIGN_FAILED_SUBJECT, externalSystemName),
                    mp, Collections.singleton(creator.getEmail()));
            log.info(String.format("Sending PLS playLaunch failed email.", creator.getEmail()));

        } catch (Exception e) {
            log.error(String.format("Failed to send PLS playLaunch failed email to %s. Exception message=%s",
                    creator.getEmail(), e.getMessage()), e);
        }
    }

    @Override
    public void sendPlsCampaignCanceledEmail(User creator, String launchHistoryUrl, String playDisplayName,
            String externalSystemName, String playLaunchState, String playLaunchCreated, String currentTime) {
        try {
            log.info(
                    String.format("Sending PLS playLaunch canceled email to creator %s started.", creator.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_CAMPAIGN_CANCELED);
            builder.replaceToken("{{firstname}}", creator.getFirstName());
            builder.replaceToken("{{launchHistoryLink}}", launchHistoryUrl);
            builder.replaceToken("{{playDisplayName}}", playDisplayName);
            builder.replaceToken("{{externalSystemName}}", externalSystemName);
            builder.replaceToken("{{playLaunchState}}", playLaunchState);
            builder.replaceToken("{{playLaunchCreated}}", playLaunchCreated);
            builder.replaceToken("{{currentTime}}", currentTime);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_CAMPAIGN_CANCELED_SUBJECT),
                    mp, Collections.singleton(creator.getEmail()));
            log.info(String.format("Sending PLS playLaunch canceled email.", creator.getEmail()));

        } catch (Exception e) {
            log.error(String.format("Failed to send PLS playLaunch canceled email to %s. Exception message=%s",
                    creator.getEmail(), e.getMessage()), e);
        }
    }

    @Override
    public void sendPlsExportOrphanRecordsRunningEmail(User user, String exportID, String type) {
        try {
            log.info(String.format("Sending %s export in-progress email to %s started.", type, user.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_EXPORT_ORPHAN_RUNNING);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{requestType}}", type);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.PLS_METADATA_ORPHAN_RECORDS_EXPORT_IN_PROGRESS_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info(String.format("Sending %s export in-progress email to %s succeeded.", type, user.getEmail()));
        } catch (Exception e) {
            log.error(String.format("Failed to send %s export in-progress email to %s. Exception message=%s", type,
                    user.getEmail(), e.getMessage()), e);
        }
    }

    @Override
    public void sendPlsExportOrphanRecordsSuccessEmail(User user, String tenantName, String hostport, String url,
                                                       String exportID, String type) {
        try {
            log.info(String.format("Sending %s export complete email to %s started.", type, user.getEmail()));
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.PLS_EXPORT_ORPHAN_SUCCESS);
            builder.replaceToken("{{firstName}}", user.getFirstName());
            builder.replaceToken("{{tenantName}}", tenantName);
            builder.replaceToken("{{downloadLink}}", url);
            builder.replaceToken("{{exportID}}", exportID);
            builder.replaceToken("{{exportType}}", type);
            builder.replaceToken("{{url}}", url);
            builder.replaceToken("{{loginURL}}", hostport);
            builder.replaceToken("{{requestType}}", type);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/export-orphan-instructions.png",
                    "image/png", "instruction");
            sendMultiPartEmail(EmailSettings.PLS_METADATA_ORPHAN_RECORDS_EXPORT_SUCCESS_SUBJECT,
                    mp, Collections.singleton(user.getEmail()));
            log.info(String.format("Sending %s export complete email to %s succeeded.", type, user.getEmail()));
        } catch (Exception e) {
            log.error("Failed to send {} export complete email to {}.", type, user.getEmail(), e);
        }
    }

    private void sendEmailForEnrichInternalAttribute(User user, String hostport, String tenantName, String modelName,
                                                     List<String> internalAttributes, Template emailTemplate,
                                                     String emailSubject) throws IOException, MessagingException {
        EmailTemplateBuilder builder;
        builder = new EmailTemplateBuilder(emailTemplate);

        builder.replaceToken("{{firstname}}", user.getFirstName());

        setTokenForInternalEnrichmentList(internalAttributes, builder);

        builder.replaceToken("{{tenantname}}", tenantName);
        builder.replaceToken("{{jobtype}}", EmailSettings.PLS_INTERNAL_ATTRIBUTE_ENRICH_EMAIL_JOB_TYPE);
        builder.replaceToken("{{modelname}}", modelName);
        builder.replaceToken("{{url}}", hostport);
        builder.replaceToken("{{apppublicurl}}", hostport);

        Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
        Collection<String> recipients = new ArrayList<>();
        recipients.add(pmEmail);
        sendMultiPartEmail(String.format(emailSubject, modelName), mp, recipients);
    }

    protected void setTokenForInternalEnrichmentList(List<String> internalAttributes, EmailTemplateBuilder builder) {
        String internalEnrichmentsText = "";
        if (!CollectionUtils.isEmpty(internalAttributes)) {
            for (String attr : internalAttributes) {
                internalEnrichmentsText += attr + COMMA;
            }
        }
        if (internalEnrichmentsText.endsWith(COMMA)) {
            internalEnrichmentsText = //
                    internalEnrichmentsText.substring(0, //
                            internalEnrichmentsText.lastIndexOf(COMMA));
        }

        builder.replaceToken("{{internalenrichmentattrinutes}}", internalEnrichmentsText);
    }

    @Override
    public void sendCDLProcessAnalyzeCompletionEmail(User user, Tenant tenant, String hostport) {
        try {
            log.info("Sending CDL processanalyze complete email to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder;
            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.CDL_JOB_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.CDL_PA_COMPLETION_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending CDL processanalyze complete email to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send CDL processanalyze complete email to " + user.getEmail() + " on "
                    + tenant.getName(), e);
        }
    }

    @Override
    public void sendCDLProcessAnalyzeErrorEmail(User user, Tenant tenant, String hostport) {
        try {
            log.info("Sending PLS processanalyze error email to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder;
            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.CDL_JOB_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.CDL_PA_ERROR_EMAIL_SUBJECT, mp, Collections.singleton(user.getEmail()));
            log.info("Sending CDL processanalyze error email to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send CDL processanalyze error email to " + user.getEmail() //
                    + " on " + tenant.getName(), e);
        }
    }

    @Override
    public void sendTenantStateNoticeEmail(User user, Tenant tenant, String state, int days) {
        try {
            log.info("Sending tenant access change notice email to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder;
            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.TENANT_STATE_NOTICE);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{state}}", state);
            builder.replaceToken("{{days}}", String.valueOf(days));

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.TENANT_STATE_NOTICE_EMAIL_SUBJECT, tenant.getTenantType()), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending tenant access change email to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send tenant access change notice email to " + user.getEmail() + " on "
                    + tenant.getName(), e);
        }
    }

    @Override
    public void sendTenantRightStatusNoticeEmail(User user, Tenant tenant, int days) {
        try {
            log.info("Sending tenant access change notice email to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder;
            if (days > 0) {
                builder = new EmailTemplateBuilder(Template.TENANT_RIGHT_NOTIFY_DAYS);
            } else {
                builder = new EmailTemplateBuilder(Template.TENANT_RIGHT_DELETE);
            }
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{email}}", user.getEmail());
            builder.replaceToken("{{days}}", String.valueOf(days));

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            if (days > 0) {
                sendMultiPartEmail(EmailSettings.TENANT_RIGHT_NOTICE_SUBJECT, mp, Collections.singleton(user.getEmail()));
            } else {
                sendMultiPartEmail(EmailSettings.TENANT_RIGHT_REMOVE_SUBJECT, mp, Collections.singleton(user.getEmail()));
            }
            log.info("Sending tenant access change email to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send tenant access change notice email to " + user.getEmail() + " on "
                    + tenant.getName(), e);
        }
    }

    @Override
    public void sendS3CredentialEmail(User user, Tenant tenant, GrantDropBoxAccessResponse response, String initiator) {
        try {
            log.info("Sending s3 credentials to " + user.getEmail() + " on " + tenant.getName() + " started.");
            EmailTemplateBuilder builder;
            builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.S3_CREDENTIALS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            if (StringUtils.isNotEmpty(response.getBucket())) {
                builder.replaceToken("{{bucket}}", response.getBucket());
            }
            if (StringUtils.isNotEmpty(response.getDropBox())) {
                builder.replaceToken("{{dropfolder}}", response.getDropBox());
            }
            if (StringUtils.isNotEmpty(initiator)) {
                builder.replaceToken("{{initiator}}", initiator);
            }
            builder.replaceToken("{{helpcenterurl}}", helpCenterUrl);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.S3_CREDENTIALS_EMAIL_SUBJECT, mp, Collections.singleton(user.getEmail()));
            log.info("Sending s3 credentials email to " + user.getEmail() + " on " + tenant.getName() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send s3 credentials email to " + user.getEmail() + " on " + tenant.getName(), e);
        }
    }

    @Override
    public void sendIngestionStatusEmail(User user, Tenant tenant, String hostport, String status,
                                         S3ImportEmailInfo emailInfo) {
        try {
            log.info("Sending cdl ingestion status " + status + " to " + user.getEmail() + " on " + tenant.getName()
                    + " " + "started.");
            EmailTemplateBuilder builder;
            if ("Success".equalsIgnoreCase(status)) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.CDL_INGESTION_SUCCESS);
            } else if ("Failed".equalsIgnoreCase(status)) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.CDL_INGESTION_ERROR);
            } else {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.CDL_INGESTION_IN_PROCESS);
            }
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{templatename}}", emailInfo.getTemplateName());
            if (StringUtils.isNotEmpty(emailInfo.getErrorMsg())) {
                builder.replaceToken("{{errormessage}}", emailInfo.getErrorMsg());
            } else {
                builder.replaceToken("{{errormessage}}", "N/A");
            }
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{type}}", emailInfo.getEntityType().getDisplayName());
            if (StringUtils.isNotEmpty(emailInfo.getFileName())) {
                builder.replaceToken("{{filename}}", emailInfo.getFileName());
            }
            builder.replaceToken("{{dropfolder}}", emailInfo.getDropFolder());
            if (emailInfo.getTimeReceived() != null) {
                String dateStr = DateTimeUtils.format(emailInfo.getTimeReceived());
                builder.replaceToken("{{timereceived}}", dateStr);
            }
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.CDL_INGESTION_STATUS_SUBJECT, status.toUpperCase()), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending cdl ingestion status " + status + " email to " + user.getEmail() + " on "
                    + tenant.getName() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send cdl ingestion status email to " + user.getEmail() + " on " + tenant.getName(), e);
        }
    }

    @Override
    public void sendS3TemplateCreateEmail(User user, Tenant tenant, String hostport, S3ImportEmailInfo emailInfo) {
        try {
            log.info("Sending s3 template create notification to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.S3_TEMPLATE_CREATE);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{templatename}}", emailInfo.getTemplateName());
            builder.replaceToken("{{type}}", emailInfo.getEntityType().getDisplayName());
            builder.replaceToken("{{dropfolder}}", emailInfo.getDropFolder());
            builder.replaceToken("{{useremail}}", emailInfo.getUser());
            builder.replaceToken("{{tenantname}}", tenant.getName());

            builder.replaceToken("{{url}}", hostport);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(EmailSettings.S3_TEMPLATE_CREATE_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending s3 template create notification to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send s3 template create notification to " + user.getEmail() + " on " + tenant.getName(), e);
        }
    }

    @Override
    public void sendS3TemplateUpdateEmail(User user, Tenant tenant, String hostport, S3ImportEmailInfo emailInfo) {
        try {
            log.info("Sending s3 template update notification to " + user.getEmail() + " on " + tenant.getName()
                    + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.S3_TEMPLATE_UPDATE);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{templatename}}", emailInfo.getTemplateName());
            builder.replaceToken("{{type}}", emailInfo.getEntityType().getDisplayName());
            builder.replaceToken("{{dropfolder}}", emailInfo.getDropFolder());
            builder.replaceToken("{{useremail}}", emailInfo.getUser());
            builder.replaceToken("{{tenantname}}", tenant.getName());

            builder.replaceToken("{{url}}", hostport);
            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.S3_TEMPLATE_UPDATE_SUBJECT, emailInfo.getTemplateName()), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending s3 template update notification to " + user.getEmail() + " on " + tenant.getName()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send s3 template update notification to " + user.getEmail() + " on " + tenant.getName(), e);
        }
    }

    @Override
    public void sendPlsActionCancelSuccessEmail(User user, String hostport,
                                                CancelActionEmailInfo cancelActionEmailInfo) {
        try {
            if (user != null) {
                log.info("Sending PLS action cancel success email to " + user.getEmail() + " started.");
                EmailTemplateBuilder builder = new EmailTemplateBuilder(
                        EmailTemplateBuilder.Template.PLS_CANCEL_ACTION_SUCCESS);

                builder.replaceToken("{{tenantname}}", cancelActionEmailInfo.getTenantName());
                builder.replaceToken("{{username}}", user.getFirstName());
                builder.replaceToken("{{actionname}}", cancelActionEmailInfo.getActionName());
                builder.replaceToken("{{actionusername}}", cancelActionEmailInfo.getActionUserName());
                builder.replaceToken("{{url}}", hostport);

                Multipart mp = builder.buildMultipart();
                sendMultiPartEmail(EmailSettings.PLS_ACTION_CANCEL_SUCCESS_EMAIL_SUBJECT,
                        mp, Collections.singleton(user.getEmail()));
                log.info("Sending PLS action cancel success email to " + user.getEmail() + " succeeded.");
            }
        } catch (Exception e) {
            log.error("Failed to send PLS action cancel success email to " + user.getEmail(), e);
        }
    }

    @Override
    public void sendUploadCompletedEmail(UploadEmailInfo uploadEmailInfo) {
        try {
            if (uploadEmailInfo.getRecipientList() != null && !uploadEmailInfo.getRecipientList().isEmpty()) {
                log.info("Sending upload completed email to " + uploadEmailInfo.getRecipientList().toString() + " started.");

                EmailTemplateBuilder builder = new EmailTemplateBuilder(
                        EmailTemplateBuilder.Template.DCP_UPLOAD_COMPLETED);

                builder.replaceToken("{{uploadDisplayName}}", uploadEmailInfo.getUploadDisplayName());
                builder.replaceToken("{{sourceDisplayName}}", uploadEmailInfo.getSourceDisplayName());
                builder.replaceToken("{{projectDisplayName}}", uploadEmailInfo.getProjectDisplayName());
                builder.replaceToken("{{url}}", dcpPublicUrl);
                Multipart mp = builder.buildRawMultipart();
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/dnb_connect.png",
                        "image/png", "dnb_connect_logo");
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/avatar.png",
                        "image/png", "avatar");
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/green-check-no-outline.png",
                        "image/png", "status_icon");

                sendMultiPartEmail(EmailSettings.DCP_UPLOAD_COMPLETED_SUBJECT,
                        mp, uploadEmailInfo.getRecipientList(), EmailFromAddress.DNB_CONNECT);
                log.info("Sending upload completed email to " + uploadEmailInfo.getRecipientList().toString() + " succeeded.");
            }
        } catch (Exception e) {
            log.error("Failed to send upload completed email to " + uploadEmailInfo.getRecipientList().toString(), e);
        }
    }

    @Override
    public void sendUploadFailedEmail(UploadEmailInfo uploadEmailInfo) {
        try {
            if (uploadEmailInfo.getRecipientList() != null && !uploadEmailInfo.getRecipientList().isEmpty()) {
                log.info("Sending upload failed email to " + uploadEmailInfo.getRecipientList().toString() + " started.");

                EmailTemplateBuilder builder = new EmailTemplateBuilder(
                        Template.DCP_UPLOAD_FAILED);

                builder.replaceToken("{{uploadDisplayName}}", uploadEmailInfo.getUploadDisplayName());
                builder.replaceToken("{{sourceDisplayName}}", uploadEmailInfo.getSourceDisplayName());
                builder.replaceToken("{{projectDisplayName}}", uploadEmailInfo.getProjectDisplayName());
                builder.replaceToken("{{url}}", dcpPublicUrl);
                Multipart mp = builder.buildRawMultipart();
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/dnb_connect.png",
                        "image/png", "dnb_connect_logo");
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/avatar.png",
                        "image/png", "avatar");
                builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/alert-big.png",
                        "image/png", "status_icon");

                sendMultiPartEmail(EmailSettings.DCP_UPLOAD_FAILED_SUBJECT,
                        mp, uploadEmailInfo.getRecipientList(), EmailFromAddress.DNB_CONNECT);
                log.info("Sending upload failed email to " + uploadEmailInfo.getRecipientList().toString() + " succeeded.");
            }
        } catch (Exception e) {
            log.error("Failed to send upload failed email to " + uploadEmailInfo.getRecipientList().toString(), e);
        }
    }

    @Override
    public Long sendDCPWelcomeEmail(User user, String tenantName, String url) {
        Long success = null;
        try {
            log.info("Sending welcome email to " + user.getEmail().toString() + " started.");

            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.DCP_WELCOME_NEW_USER);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenantName);
            builder.replaceToken("{{url}}", url);
            Multipart multipart = builder.buildRawMultipart();
            builder.addCustomImagesToMultipart(multipart, "com/latticeengines/monitor/dnb_connect.png",
                    "image/png", "dnb_connect_logo");
            builder.addCustomImagesToMultipart(multipart, "com/latticeengines/monitor/avatar.png",
                    "image/png", "avatar");

            sendMultiPartEmail(EmailSettings.DNB_CONNECT_WELCOME_NEW_USER_SUBJECT, multipart, Collections.singleton(user.getEmail()), EmailFromAddress.DNB_CONNECT);
            log.info("Sending welcome email to " + user.getEmail().toString() + "succeeded.");
            success = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Failed to send welcome email to " + user.getEmail().toString(), e);
        }

        return success;
    }

    @Override
    public Long sendDCPWelcomeEmail(IDaaSUser idaasUser, String tenantName, String url) {
        User user = new User();
        user.setEmail(idaasUser.getEmailAddress());
        user.setFirstName(idaasUser.getFirstName());

        return sendDCPWelcomeEmail(user, tenantName, url);
    }

    @Override
    public void sendDnbIntentAlertEmail(@NotNull Tenant tenant, @NotNull Collection<String> recipients,
            @NotNull String subject, @NotNull Map<String, Object> params) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(subject);
        Preconditions.checkNotNull(recipients);
        Preconditions.checkNotNull(params);
        String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
        log.info("Sending dnb intent alert email to {} recipients, with subject {} for tenant {}",
                CollectionUtils.size(recipients), subject, tenantId);
        Multipart multipart = buildDnBIntentAlertEmailBody(tenantId, params);
        sendMultiPartEmail(subject, multipart, recipients);
    }

    private Multipart buildDnBIntentAlertEmailBody(@NotNull String tenantId, @NotNull Map<String, Object> params) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(Template.DNB_INTENT_ALERT).renderTemplate(params);

            // TODO minify html in email body
            Multipart mp = builder.buildRawMultipart();
            // TODO change logo to dnb lattice logo when UX finishes it
            builder.addCustomImagesToMultipart(mp, "com/latticeengines/monitor/dnb_logo.svg", "image/svg+xml", "logo");
            if (params.get("attachment") != null) {
                builder.addAttachmentToMultipart(mp, (byte[]) params.get("attachment"),
                        params.get("attachment_name").toString(), "text/csv");
            }
            return mp;
        } catch (IOException | MessagingException e) {
            String msg = String.format("Failed to generate dnb intent email for tenant %s. params = %s", tenantId,
                    params);
            throw new RuntimeException(msg, e);
        }
    }

}

