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
        } catch (Exception e) {
            log.error("Failed to send new internal user email to " + user.getEmail() + " " + e.getMessage());
        }
        log.info("Sending new internal user email to " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPlsNewExternalUserEmail(User user, String password, String hostport) {
        try {
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
        } catch (Exception e) {
            log.error("Failed to send new external user email to " + user.getEmail() + " " + e.getMessage());
        }
        log.info("Sending new external user email to " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPlsExistingInternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXISTING_INTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp, Collections.singleton(user.getEmail()));
        } catch (Exception e) {
            log.error("Failed to send existing internal user email to " + user.getEmail() + " " + e.getMessage());
        }
        log.info("Sending existing internal user email to " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPlsExistingExternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PLS_EXISTING_EXTERNAL_USER);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp, Collections.singleton(user.getEmail()));
        } catch (Exception e) {
            log.error("Failed to send existing external user email to " + user.getEmail() + " " + e.getMessage());
        }
        log.info("Sending existing external user email to " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPlsForgetPasswordEmail(User user, String password, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PLS_FORGET_PASSWORD);

            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            sendMultiPartEmail("Lattice Password Reset", mp, Collections.singleton(user.getEmail()));
        } catch (Exception e) {
            log.error("Failed to send forget password email to " + user.getEmail() + " " + e.getMessage());
        }
        log.info("Sending forget password email " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPdNewExternalUserEmail(User user, String password, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(EmailTemplateBuilder.Template.PD_NEW_EXTERNAL_USER);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantmsg}}", "You have been granted access to the Lattice Prospect Discovery.");
            builder.replaceToken("{{username}}", user.getUsername());
            builder.replaceToken("{{tenantname}}", "&nbsp;");
            builder.replaceToken("{{password}}", password);
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            log.info("Sending email to " + user.getUsername());
            sendMultiPartEmail("Welcome to Lattice Prospect Discovery", mp, Collections.singleton(user.getEmail()));
        } catch (Exception e) {
            log.error(String.format("Failed to send new external user email for PD to %s with the error message: %s",
                    user.getEmail(), e.getMessage()));
        }
        log.info("Sending new external user email to " + user.getEmail() + " succeeded.");
    }

    @Override
    public void sendPdExistingExternalUserEmail(Tenant tenant, User user, String hostport) {
        try {
            EmailTemplateBuilder builder = new EmailTemplateBuilder(
                    EmailTemplateBuilder.Template.PD_EXISITING_EXTERNAL_USER);
            builder.replaceToken("{{firstname}}", user.getFirstName());
            builder.replaceToken("{{lastname}}", user.getLastName());
            builder.replaceToken("{{tenantname}}", tenant.getName());
            builder.replaceToken("{{url}}", hostport);

            Multipart mp = builder.buildMultipart();
            log.info("Sending email to " + user.getUsername());
            sendMultiPartEmail("Welcome to Lattice Prospect Discovery", mp, Collections.singleton(user.getEmail()));
        } catch (Exception e) {
            log.error(String.format(
                    "Failed to send exisiting external user email for PD to %s with the error message: %s",
                    user.getEmail(), e.getMessage()));
        }
        log.info("Sending existing external user email to " + user.getEmail() + " succeeded.");
    }

}
