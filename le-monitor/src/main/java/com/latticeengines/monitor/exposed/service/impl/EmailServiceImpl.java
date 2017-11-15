package com.latticeengines.monitor.exposed.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.monitor.EmailSettings;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.util.EmailTemplateBuilder;
import com.latticeengines.monitor.util.EmailTemplateBuilder.Template;
import com.latticeengines.monitor.util.EmailUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;

@Component
public class EmailServiceImpl implements EmailService {

    private static final Logger log = LoggerFactory.getLogger(EmailServiceImpl.class);

    private static final String COMMA = ", ";

    @Autowired
    private EmailSettings emailsettings;

    @Value("${monitor.email.enabled:true}")
    private boolean emailEnabled;

    @Value("${monitor.email.businessops:businessops@lattice-engines.com}")
    private String businessOpsEmail;

    @Value("${monitor.email.pm:dev@lattice-engines.com}")
    private String pmEmail;

    @VisibleForTesting
    public void enableEmail() {
        emailEnabled = true;
    }

    @Override
    public void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients) {
        if (emailEnabled) {
            EmailUtils.sendSimpleEmail(subject, content, contentType, recipients, emailsettings);
        }
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients) {
        if (emailEnabled) {
            EmailUtils.sendMultiPartEmail(subject, content, recipients, null, emailsettings);
        }
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients,
            Collection<String> bccRecipients) {
        if (emailEnabled) {
            EmailUtils.sendMultiPartEmail(subject, content, recipients, bccRecipients, emailsettings);
        }
    }

    @Override
    public void sendPlsNewInternalUserEmail(Tenant tenant, User user, String password, String hostport) {
        try {
            log.info("Sending new PLS internal user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_NEW_INTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantmsg}}",
                    String.format(EmailSettings.PLS_NEW_INTERNAL_USER_EMAIL_MSG, tenant.getName()));
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()));
            log.info("Sending new PLS internal user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send new PLS internal user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsNewExternalUserEmail(User user, String password, String hostport, boolean bccEmail) {
        try {
            log.info("Sending new PLS external user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_NEW_EXTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantmsg}}", EmailSettings.PLS_NEW_EXTERNAL_USER_EMAIL_MSG);
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipart();
            log.info("Sending email to " + user.getUsername());
            if (bccEmail) {
                sendMultiPartEmail(EmailSettings.PLS_NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()),
                        Collections.singleton(businessOpsEmail));
            } else {
                sendMultiPartEmail(EmailSettings.PLS_NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()));
            }
            log.info("Sending new PLS external user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send new PLS external user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsExistingInternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            log.info("Sending existing PLS internal user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXISTING_INTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_EXISTING_USER_SUBJECT, tenant.getName()), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending existing PLS internal user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send existing PLS internal user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsExistingExternalUserEmail(Tenant tenant, User user, String hostport, boolean bccEmail) {
        try {
            log.info("Sending existing PLS external user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXISTING_EXTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            if (bccEmail) {
                sendMultiPartEmail(String.format(EmailSettings.PLS_EXISTING_USER_SUBJECT, tenant.getName()), mp,
                        Collections.singleton(user.getEmail()), Collections.singleton(businessOpsEmail));
            } else {
                sendMultiPartEmail(String.format(EmailSettings.PLS_EXISTING_USER_SUBJECT, tenant.getName()), mp,
                        Collections.singleton(user.getEmail()));
            }
            log.info("Sending PLS existing external user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send existing PLS external user email to " + user.getEmail() + " " + e.getMessage());
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
            log.error("Failed to send forget password email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPdNewExternalUserEmail(User user, String password, String hostport) {
        try {
            log.info("Sending new PD external user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PD_NEW_EXTERNAL_USER);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantmsg}}", EmailSettings.PD_NEW_USER_EMAIL_MSG);
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{tenantname}}", "&nbsp;");
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PD_NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()));
            log.info("Sending new PD external user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error(String.format("Failed to send new PD external user email for to %s with the error message: %s",
                    user.getEmail(), e.getMessage()));
        }
    }

    @Override
    public void sendPdExistingExternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            log.info("Sending existing PD external user email to " + user.getEmail() + " succeeded.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_EXISITING_EXTERNAL_USER);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PD_NEW_USER_SUBJECT, mp, Collections.singleton(user.getEmail()));
            log.info("Sending existing PD external user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error(String.format(
                    "Failed to send exisiting PD external user email for to %s with the error message: %s",
                    user.getEmail(), e.getMessage()));
        }
    }

    @Override
    public void sendPlsImportDataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PLS import data complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", EmailSettings.PLS_IMPORT_DATA_SUCCESS_EMAIL_MSG);
            builder.replaceToken("{{currentstep}}", EmailSettings.PLS_IMPORT_DATA_SUCCESS_CURRENT_STEP);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_IMPORT_DATA_SUCCESS_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS import data complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS import data complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsImportDataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PLS import data error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{job}}", EmailSettings.PLS_ERROR_EMAIL_LINK_JOB);
            builder.replaceToken("{{errormsg}}", EmailSettings.PLS_IMPORT_DATA_ERROR_EMAIL_MSG);
            builder.replaceToken("{{linkmsg}}", EmailSettings.PLS_ERROR_EMAIL_LINK_MSG);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_IMPORT_DATA_ERROR_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS import data error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS import data error email to " + user.getEmail() + " " + e.getMessage());
        }

    }

    @Override
    public void sendPlsEnrichDataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PLS enrichment data complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", EmailSettings.PLS_ENRICH_DATA_SUCCESS_EMAIL_MSG);
            builder.replaceToken("{{currentstep}}", EmailSettings.PLS_ENRICH_DATA_SUCCESS_EMAIL_CURRENT_STEP);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_ENRICH_DATA_SUCCESS_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS enrichment data complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrichment data complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsEnrichDataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PLS enrich data error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{job}}", EmailSettings.PLS_ERROR_EMAIL_LINK_JOB);
            builder.replaceToken("{{errormsg}}", EmailSettings.PLS_ENRICH_DATA_ERROR_EMAIL_MSG);
            builder.replaceToken("{{linkmsg}}", EmailSettings.PLS_ERROR_EMAIL_LINK_MSG);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_ENRICH_DATA_ERROR_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS enrich data error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrich data error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsValidateMetadataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PLS validate metadata complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", EmailSettings.PLS_VALIDATE_METADATA_SUCCESS_EMAIL_MSG);
            builder.replaceToken("{{currentstep}}", "");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_VALIDATE_METADATA_SUCCESS_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS validate metadata complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error(
                    "Failed to send PLS validate metadata complete email to " + user.getEmail() + " " + e.getMessage());
        }

    }

    @Override
    public void sendPlsMetadataMissingEmail(User user, String hostport) {
        try {
            log.info("Sending PLS metadata missing email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{job}}", EmailSettings.PLS_ERROR_EMAIL_LINK_JOB);
            builder.replaceToken("{{errormsg}}", EmailSettings.PLS_METADATA_MISSING_EMAIL_MSG);
            builder.replaceToken("{{linkmsg}}", EmailSettings.PLS_METADATA_MISSING_EMAIL_LINK_MSG);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_METADATA_MISSING_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS metadata missing email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS metadata missing email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsValidateMetadataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PLS validate metadata error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{job}}", EmailSettings.PLS_ERROR_EMAIL_LINK_JOB);
            builder.replaceToken("{{errormsg}}", EmailSettings.PLS_VALIDATE_METADATA_ERROR_EMAIL_MSG);
            builder.replaceToken("{{linkmsg}}", EmailSettings.PLS_ERROR_EMAIL_LINK_MSG);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(EmailSettings.PLS_VALIDATE_METADATA_ERROR_EMAIL_SUBJECT, mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS validate metadata error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS validate metadata error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsCreateModelCompletionEmail(User user, String hostport, String tenantName, String modelName,
            boolean internal) {
        try {
            log.info("Sending PLS create model (" + modelName + ") complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;
            if (internal) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS_INTERNAL);
            } else {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS);
            }

            builder.replaceToken("{{firstname}}", user.getFirstName());
            if (internal) {
                builder.replaceToken("{{tenantname}}", tenantName);
            }
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_CREATE_MODEL_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_CREATE_MODEL_COMPLETION_EMAIL_SUBJECT, modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model (" + modelName + ") complete email to " + user.getEmail() + " "
                    + e.getMessage());
        }
    }

    @Override
    public void sendPlsCreateModelErrorEmail(User user, String hostport, String tenantName, String modelName,
            boolean internal) {
        try {
            log.info("Sending PLS create model (" + modelName + ") error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;
            if (internal) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR_INTERNAL);
            } else {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR);
            }

            builder.replaceToken("{{firstname}}", user.getFirstName());
            if (internal) {
                builder.replaceToken("{{tenantname}}", tenantName);
            }
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_CREATE_MODEL_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_CREATE_MODEL_ERROR_EMAIL_SUBJECT, modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model (" + modelName + ") error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model (" + modelName + ") error email to " + user.getEmail() + " "
                    + e.getMessage());
        }
    }

    @Override
    public void sendPlsScoreCompletionEmail(User user, String hostport, String tenantName, String modelName,
            boolean internal) {
        try {
            log.info("Sending PLS scoring (" + modelName + ") complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;
            if (internal) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS_INTERNAL);
            } else {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_SUCCESS);
            }

            builder.replaceToken("{{firstname}}", user.getFirstName());
            if (internal) {
                builder.replaceToken("{{tenantname}}", tenantName);
            }
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_SCORE_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_SCORE_COMPLETION_EMAIL_SUBJECT, modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create scoring (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring (" + modelName + ") complete email to " + user.getEmail() + " "
                    + e.getMessage());
        }
    }

    @Override
    public void sendPlsScoreErrorEmail(User user, String hostport, String tenantName, String modelName,
            boolean internal) {
        try {
            log.info("Sending PLS scoring (" + modelName + ") error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder;
            if (internal) {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR_INTERNAL);
            } else {
                builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_JOB_ERROR);
            }

            builder.replaceToken("{{firstname}}", user.getFirstName());
            if (internal) {
                builder.replaceToken("{{tenantname}}", tenantName);
            }
            builder.replaceToken("{{jobtype}}", EmailSettings.PLS_SCORE_EMAIL_JOB_TYPE);
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format(EmailSettings.PLS_SCORE_ERROR_EMAIL_SUBJECT, modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info(
                    "Sending PLS create scoring (" + modelName + ") error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring (" + modelName + ") error email to " + user.getEmail() + " "
                    + e.getMessage());
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
                    user.getEmail(), tenantId));
        }
    }

    @Override
    public void sendGlobalAuthForgetCredsEmail(String firstName, String lastName, String username, String password,
            String emailAddress, EmailSettings settings) {
        try {
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
            String modelName, boolean internal, List<String> internalAttributes) {
        try {
            log.info("Sending PLS enrich internal attribute (" + modelName + ") complete email to " + user.getEmail()
                    + " started.");
            sendEmailForEnrichInternalAttribute(user, hostport, tenantName, modelName, internal, internalAttributes,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS_INTERNAL,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS,
                    EmailSettings.PLS_INTERNAL_ATTRIBUTE_ENRICH_COMPLETION_EMAIL_SUBJECT);
            log.info("Sending PLS enrich internal attribute (" + modelName + ") complete email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrich internal attribute (" + modelName + ") complete email to "
                    + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsEnrichInternalAttributeErrorEmail(User user, String hostport, String tenantName,
            String modelName, boolean internal, List<String> internalAttributes) {
        try {
            log.info("Sending PLS enrich internal attribute (" + modelName + ") error email to " + user.getEmail()
                    + " started.");
            sendEmailForEnrichInternalAttribute(user, hostport, tenantName, modelName, internal, internalAttributes,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_INTERNAL,
                    EmailTemplateBuilder.Template.PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR,
                    EmailSettings.PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_EMAIL_SUBJECT);
            log.info("Sending PLS enrich internal attribute (" + modelName + ") error email to " + user.getEmail()
                    + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS enrich internal attribute (" + modelName + ") error email to "
                    + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsExportSegmentSuccessEmail(User user, String hostport, String exportID, String type, Date cleanupBy) {
        try {
            log.info("Sending segment export complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXPORT_SEGMENT_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{downloadLink}}", hostport);
            builder.replaceToken("{{exportID}}", exportID);
            builder.replaceToken("{{exportType}}", type);
            builder.replaceToken("{{date}}", DateTimeUtils.convertToStringUTCISO8601(cleanupBy));
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(String.format(EmailSettings.PLS_METADATA_SEGMENT_EXPORT_SUCCESS_SUBJECT, exportID), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS segment export complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS export segment complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsExportSegmentErrorEmail(User user, String hostport, String exportID, String type) {
        try {
            log.info("Sending PLS export segment error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXPORT_SEGMENT_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(String.format(EmailSettings.PLS_METADATA_SEGMENT_EXPORT_ERROR_SUBJECT, exportID), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS export segment error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS export segment error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    private void sendEmailForEnrichInternalAttribute(User user, String hostport, String tenantName, String modelName,
            boolean internal, List<String> internalAttributes, Template internalEmailTemplate, Template emailTemplate,
            String emailSubject) throws IOException, MessagingException {
        EmailTemplateBuilder builder;
        if (internal) {
            builder = new EmailTemplateBuilder(internalEmailTemplate);
        } else {
            builder = new EmailTemplateBuilder(emailTemplate);
        }

        builder.replaceToken("{{firstname}}", user.getFirstName());

        setTokenForInternalEnrichmentList(internalAttributes, builder);

        if (internal) {
            builder.replaceToken("{{tenantname}}", tenantName);
        }

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

}
