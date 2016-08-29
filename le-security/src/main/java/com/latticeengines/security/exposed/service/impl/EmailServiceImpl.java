package com.latticeengines.security.exposed.service.impl;

import java.util.Collection;
import java.util.Collections;

import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.EmailSettings;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.util.EmailTemplateBuilder;
import com.latticeengines.security.util.EmailUtils;

@Component
public class EmailServiceImpl implements EmailService {

    private static final Log log = LogFactory.getLog(EmailServiceImpl.class);

    @Autowired
    private EmailSettings emailsettings;

    @Value("${security.email.enabled:true}")
    private boolean emailEnabled;

    @Value("${security.email.businessops:businessops@lattice-engines.com}")
    private String businessOpsEmail;

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
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients, Collection<String> bccRecipients) {
        if (emailEnabled) {
            EmailUtils.sendMultiPartEmail(subject, content, recipients, bccRecipients, emailsettings);
        }
    }

    @Override
    public void sendPlsNewInternalUserEmail(Tenant tenant, User user, String password, String hostport) {
        try {
            log.info("Sending new PLS internal user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_NEW_INTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken(
                    "{{tenantmsg}}",
                    String.format("You have been added to the <strong>%s</strong> Lead Prioritization Tenant.",
                            tenant.getName()));
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Predictive Insights", mp, Collections.singleton(user.getEmail()));
            log.info("Sending new PLS internal user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send new PLS internal user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsNewExternalUserEmail(User user, String password, String hostport) {
        try {
            log.info("Sending new PLS external user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_NEW_EXTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantmsg}}",
                    "Congratulations! You've been invited to use Lattice Predicative Insights. You can sign in for the first time using the temporary credentials listed below.\n");
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipart();
            log.info("Sending email to " + user.getUsername());
            sendMultiPartEmail("Welcome to Lattice Predictive Insights", mp, Collections.singleton(user.getEmail()),
                    Collections.singleton(businessOpsEmail));
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

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(String.format("Invitation to Access %s (Lattice Predictive Insights)",
                    tenant.getName()), mp, Collections.singleton(user.getEmail()));
            log.info("Sending existing PLS internal user email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send existing PLS internal user email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsExistingExternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            log.info("Sending existing PLS external user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXISTING_EXTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail(String.format("Invitation to Access %s (Lattice Predictive Insights)",
                    tenant.getName()), mp, Collections.singleton(user.getEmail()));
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
            sendMultiPartEmail("Password Reset for Lattice Predictive Insights", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{tenantmsg}}", "You have been granted access to the Lattice Prospect Discovery.");
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{tenantname}}", "&nbsp;");
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Prospect Discovery", mp, Collections.singleton(user.getEmail()));
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
            sendMultiPartEmail("Welcome to Lattice Prospect Discovery", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{completemsg}}", "Your Salesforce Data Import is complete.");
            builder.replaceToken("{{currentstep}}",
                    "The system is currently enriching the data with Lattice Data Cloud.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Import Data Successfully", mp,
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
            builder.replaceToken("{{job}}", "tenant deployment");
            builder.replaceToken("{{errormsg}}", "Unable to import data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Import Data Failed", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{completemsg}}", "Your Data Enrichment is complete.");
            builder.replaceToken("{{currentstep}}", "The system is currently validating metadata.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Enrich Data Successfully", mp,
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
            builder.replaceToken("{{job}}", "tenant deployment");
            builder.replaceToken("{{errormsg}}", "Unable to enrich data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Enrich Data Failed", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{completemsg}}", "Your Metadata Validation is complete.");
            builder.replaceToken("{{currentstep}}", "");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Metadata Validation Successfully", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS validate metadata complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS validate metadata complete email to " + user.getEmail() + " "
                    + e.getMessage());
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
            builder.replaceToken("{{job}}", "tenant deployment");
            builder.replaceToken("{{errormsg}}", "Missing data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to add missing data.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Metadata Missing", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{job}}", "tenant deployment");
            builder.replaceToken("{{errormsg}}", "Unable to validate metadata.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Validate Metadata Failed", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS validate metadata error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS validate metadata error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsCreateModelCompletionEmail(User user, String hostport, String tenantName, String modelName, boolean internal) {
        try {
            log.info("Sending PLS create model complete email to " + user.getEmail() + " started.");
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
            builder.replaceToken("{{jobtype}}", "Create Model");
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{completemsg}}", "We have completed the model creation.");
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format("SUCCESS - Create Model - %s ", modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsCreateModelErrorEmail(User user, String hostport, String tenantName, String modelName, boolean internal) {
        try {
            log.info("Sending PLS create model error email to " + user.getEmail() + " started.");
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
            builder.replaceToken("{{jobtype}}", "Create Model");
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{errormsg}}", "Failed to create a model.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format("FAILURE - Create Model - %s ", modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create model error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS create model error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsScoreCompletionEmail(User user, String hostport, String tenantName, String modelName, boolean internal) {
        try {
            log.info("Sending PLS scoring complete email to " + user.getEmail() + " started.");
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
            builder.replaceToken("{{jobtype}}", "Score List");
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{completemsg}}", "We have completed the scoring.");
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format("SUCCESS - Score List - %s ", modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create scoring email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPlsScoreErrorEmail(User user, String hostport, String tenantName, String modelName, boolean internal) {
        try {
            log.info("Sending PLS scoring error email to " + user.getEmail() + " started.");
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
            builder.replaceToken("{{jobtype}}", "Score List");
            builder.replaceToken("{{modelname}}", modelName);
            builder.replaceToken("{{errormsg}}", "Failed to score.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);
            builder.replaceToken("{{apppublicurl}}", hostport);

            Multipart mp = builder.buildMultipartWithoutWelcomeHeader();
            sendMultiPartEmail(String.format("FAILURE - Score List - %s ", modelName), mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PLS create scoring email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PLS scoring error email to " + user.getEmail() + " " + e.getMessage());
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
            sendMultiPartEmail("Salesforce Access Token", mp, Collections.singleton(user.getEmail()));
            log.info(String.format("Sending PLS one-time SFDC access token to: %s for tenant: %s succeeded", user.getEmail(), tenantId));
        } catch (Exception e) {
            log.error(String.format("Sending PLS one-time SFDC access token to: %s for tenant: %s failed", user.getEmail(), tenantId));
        }
    }

    @Override
    public void sendGlobalAuthForgetCredsEmail(String firstName, String lastName, String username,
            String password, String emailAddress, EmailSettings settings) {
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
            EmailUtils.sendMultiPartEmail("Lattice Password Reset", mp,
                    Collections.singleton(emailAddress), null, settings);
        } catch (Exception e) {
            log.error(String.format("Sending global auth forget credentials email to :%s failed", emailAddress));
        }
    }
}
