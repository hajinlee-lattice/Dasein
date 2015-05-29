package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.Collections;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

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

    public boolean sendNewInternalUserEmail(Tenant tenant, User user, String password) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/new_user.html"));

            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{tenantmsg}}",
                    String.format("You have been added to the <strong>%s</strong> Lead Prioritization Tenant.",
                            tenant.getName()));
            htmlTemplate = htmlTemplate.replace("{{username}}", user.getUsername());
            htmlTemplate = htmlTemplate.replace("{{password}}", password);
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(htmlTemplate, "text/html");
            mp.addBodyPart(htmlPart);
            appendImagesToMultipart(mp);

            emailService.sendMultiPartEmail("Welcome to Lead Prioritization", mp, Collections.singleton(user.getEmail()));
            return true;
        } catch (Exception e) {
            log.error("Failed to send new internal user email to " + user.getEmail() + " " + e.getMessage());
            return false;
        }
    }

    public boolean sendNewExternalUserEmail(User user, String password) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/new_user.html"));

            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{tenantmsg}}",
                    "You have been granted access to the Lattice Lead Prioritization App.");
            htmlTemplate = htmlTemplate.replace("{{username}}", user.getUsername());
            htmlTemplate = htmlTemplate.replace("{{tenantname}}", "&nbsp;");
            htmlTemplate = htmlTemplate.replace("{{password}}", password);
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(htmlTemplate, "text/html");
            mp.addBodyPart(htmlPart);
            appendImagesToMultipart(mp);

            emailService.sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp,
                    Collections.singleton(user.getEmail()));
            return true;
        } catch (Exception e) {
            log.error("Failed to send new external user email to " + user.getEmail() + " " + e.getMessage());
            return false;
        }
    }

    public boolean sendExistingInternalUserEmail(Tenant tenant, User user) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/old_user.html"));
            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{tenantname}}",tenant.getName());
            htmlTemplate = htmlTemplate.replace("{{appname}}","Lead Prioritization Tenant");
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(htmlTemplate, "text/html");
            mp.addBodyPart(htmlPart);
            appendImagesToMultipart(mp);

            emailService.sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp,
                    Collections.singleton(user.getEmail()));
            return true;
        } catch (Exception e) {
            log.error("Failed to send existing external user email to " + user.getEmail() + " " + e.getMessage());
            return false;
        }
    }

    public boolean sendExistingExternalUserEmail(Tenant tenant, User user) {
        try {
            String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/service/old_user.html"));
            htmlTemplate = htmlTemplate.replace("{{firstname}}", user.getFirstName());
            htmlTemplate = htmlTemplate.replace("{{lastname}}", user.getLastName());
            htmlTemplate = htmlTemplate.replace("{{tenantname}}",tenant.getName());
            htmlTemplate = htmlTemplate.replace("{{appname}}","Tenant in Lattice Lead Prioritization App");
            htmlTemplate = htmlTemplate.replace("{{url}}", hostport);

            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(htmlTemplate, "text/html");
            mp.addBodyPart(htmlPart);
            appendImagesToMultipart(mp);

            emailService.sendMultiPartEmail("Welcome to Lattice Lead Prioritization", mp,
                    Collections.singleton(user.getEmail()));
            return true;
        } catch (Exception e) {
            log.error("Failed to send existing external user email to " + user.getEmail() + " " + e.getMessage());
            return false;
        }
    }

    private void appendImagesToMultipart(Multipart mp) throws IOException, MessagingException {
        MimeBodyPart logoPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/pls/service/email_logo.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<logo>");
        mp.addBodyPart(logoPart);

        logoPart = new MimeBodyPart();
        fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/pls/service/email_banner.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<banner>");
        mp.addBodyPart(logoPart);
    }

}
