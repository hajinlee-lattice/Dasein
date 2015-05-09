package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.EmailService;

@Component("emailUtils")
public class EmailUtils {

    private static final Log log = LogFactory.getLog(EmailUtils.class);

    @Autowired
    private EmailService emailService;

    @Value("${pls.api.hostport}")
    private String hostport;

    public void sendNewInternalUserEmail(Tenant tenant, User user, String password) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/new_user.html"));

            String paragraphs = String.format(
                    "<p>You have been added to the <strong>%s</strong> Lead Prioritization Tenant.</p>",
                    tenant.getName());
            paragraphs += "<p>Use the following credentials to access the application: </p>";

            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{username}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{password}}", password);
            htmlTemplate = htmlTemplate.replace("{{paragraphs}}", paragraphs);
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            emailService.sendSimpleEmail("Welcome to Lead Prioritization",
                    htmlTemplate, "text/html; charset=utf-8",
                    Collections.singleton(user.getEmail()));
        } catch (IOException e) {
            log.error("Failed to send new internal user email: " + e.getMessage());
        }
    }

    public void sendNewExternalUserEmail(User user, String password) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/new_user.html"));

            String paragraphs = "<p>You have been been granted access to the Lattice Lead Prioritization App.</p>";
            paragraphs += "<p>Use the following credentials to access the application:</p>";

            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{username}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{password}}", password);
            htmlTemplate = htmlTemplate.replace("{{paragraphs}}", paragraphs);
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            emailService.sendSimpleEmail("Welcome to Lattice Lead Prioritization",
                    htmlTemplate, "text/html; charset=utf-8",
                    Collections.singleton(user.getEmail()));
        } catch (IOException e) {
            log.error("Failed to send new external user email: " + e.getMessage());
        }
    }

    public void sendExistingInternalUserEmail(Tenant tenant, User user) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/old_user.html"));
            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{tenantname}}",tenant.getName());
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            emailService.sendSimpleEmail("Welcome to Lattice Lead Prioritization",
                    htmlTemplate, "text/html; charset=utf-8",
                    Collections.singleton(user.getEmail()));

        } catch (IOException e) {
            log.error("Failed to send existing external user email: " + e.getMessage());
        }
    }

}
