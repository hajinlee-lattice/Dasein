package com.latticeengines.security.exposed.service.impl;

import java.util.Collection;
import java.util.Collections;

import javax.mail.Multipart;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Override
    public void sendSimpleEmail(String subject, Object content, String contentType, Collection<String> recipients) {
        EmailUtils.sendSimpleEmail(subject, content, contentType, recipients, emailsettings);
    }

    @Override
    public void sendMultiPartEmail(String subject, Multipart content, Collection<String> recipients) {
        EmailUtils.sendMultiPartEmail(subject, content, recipients, emailsettings);
    }

    @Override
    public void sendPlsNewInternalUserEmail(Tenant tenant, User user, String password, String hostport) {
        try {
            log.info("Sending new PLS internal user email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_NEW_INTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken(
                    "{{tenantmsg}}",
                    String.format("You have been added to the <strong>%s</strong> Lead Prioritization Tenant.",
                            tenant.getName()));
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lead Prioritization", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantmsg}}",
                    "You have been granted access to the Lattice Lead Prioritization App.");
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{tenantname}}", "&nbsp;");
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            log.info("Sending email to " + user.getUsername());
            sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp, Collections.singleton(user.getEmail()));
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
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lattice Password Reset", mp, Collections.singleton(user.getEmail()));
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
    public void sendPdImportDataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PD import data complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", "Your Salesforce Data Import is complete.");
            builder.replaceToken("{{currentstep}}",
                    "The system is currently enriching the data with Lattice Data Cloud.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Import Data Successfully", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PD import data complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD import data complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPdImportDataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PD import data error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{errormsg}}", "Unable to import data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Import Data Failed", mp, Collections.singleton(user.getEmail()));
            log.info("Sending PD import data error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD import data error email to " + user.getEmail() + " " + e.getMessage());
        }

    }

    @Override
    public void sendPdEnrichDataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PD enrichment data complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", "Your Data Enrichment is complete.");
            builder.replaceToken("{{currentstep}}", "The system is currently validating metadata.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Enrich Data Successfully", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PD enrichment data complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD enrichment data complete email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPdEnrichDataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PD enrich data error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{errormsg}}", "Unable to enrich data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Enrich Data Failed", mp, Collections.singleton(user.getEmail()));
            log.info("Sending PD enrich data error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD enrich data error email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPdValidateMetadataSuccessEmail(User user, String hostport) {
        try {
            log.info("Sending PD validate metadata complete email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_SUCCESS);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{completemsg}}", "Your Metadata Validation is complete.");
            builder.replaceToken("{{currentstep}}", "");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Metadata Validation Successfully", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PD validate metadata complete email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD validate metadata complete email to " + user.getEmail() + " " + e.getMessage());
        }

    }

    @Override
    public void sendPdMetadataMissingEmail(User user, String hostport) {
        try {
            log.info("Sending PD metadata missing email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{errormsg}}", "Missing data.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to add missing data.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Metadata Missing", mp, Collections.singleton(user.getEmail()));
            log.info("Sending PD metadata missing email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD metadata missing email to " + user.getEmail() + " " + e.getMessage());
        }
    }

    @Override
    public void sendPdValidateMetadataErrorEmail(User user, String hostport) {
        try {
            log.info("Sending PD validate metadata error email to " + user.getEmail() + " started.");
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_DEPLOYMENT_STEP_ERROR);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{errormsg}}", "Unable to validate metadata.");
            builder.replaceToken("{{linkmsg}}", "Sign in to Lattice to retry.");
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lead Prioritization - Validate Metadata Failed", mp,
                    Collections.singleton(user.getEmail()));
            log.info("Sending PD validate metadata error email to " + user.getEmail() + " succeeded.");
        } catch (Exception e) {
            log.error("Failed to send PD validate metadata error email to " + user.getEmail() + " " + e.getMessage());
        }
    }
}
