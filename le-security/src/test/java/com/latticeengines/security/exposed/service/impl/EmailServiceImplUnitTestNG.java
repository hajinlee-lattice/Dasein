package com.latticeengines.security.exposed.service.impl;

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
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.EmailSettings;

import junit.framework.Assert;

public class EmailServiceImplUnitTestNG {

    private EmailServiceImpl emailService = new EmailServiceImpl();
    
    @Test(groups = "unit")
    public void testSendSimpleEmail() {
        EmailSettings settings = new EmailSettings();
        settings.setServer("smtprelay.lattice.local");
        settings.setUsername("build@lattice-engines.com");
        settings.setPassword("ryTy2wEiaw51Le9qTXxx3w==");
        settings.setFrom("no-reply@lattice-engines.com");
        settings.setPort(25);
        settings.setUseSTARTTLS(false);
        settings.setUseSSL(false);

        try {
            emailService.sendSimpleEmail("Testing Subject", "This is a testing email.", "text/html",
                    Collections.singleton("ytsong@lattice-engines.com"), settings);
        } catch (Exception e) {
            Assert.fail("Failed to send email to an internal address.");
        }

        try {
            emailService.sendSimpleEmail("Testing Subject", "This is a testing email.", "text/html",
                    Collections.singleton("yintaosong@gmail.com"), settings);
        } catch (Exception e) {
            Assert.fail("Failed to send email to an external address.");
        }
    }

    @Test(groups = "unit")
    public void testSendMultipartEmail() throws MessagingException, IOException {
        EmailSettings settings = new EmailSettings();
        settings.setServer("smtprelay.lattice.local");
        settings.setUsername("build@lattice-engines.com");
        settings.setPassword("ryTy2wEiaw51Le9qTXxx3w==");
        settings.setFrom("no-reply@lattice-engines.com");
        settings.setPort(25);
        settings.setUseSTARTTLS(false);
        settings.setUseSSL(false);

        Multipart mp = new MimeMultipart();
        String htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/security/service/impl/new_user.html"));
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(htmlTemplate, "text/html");
        mp.addBodyPart(htmlPart);

        // second part (the image)
        MimeBodyPart logoPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/security/service/impl/email_logo.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<logo>");
        mp.addBodyPart(logoPart);

        logoPart = new MimeBodyPart();
        fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/security/service/impl/email_banner.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<banner>");
        mp.addBodyPart(logoPart);

        try {
            emailService.sendMultiPartEmail("Testing Subject", mp,
                    Collections.singleton("ytsong@lattice-engines.com"), settings);
        } catch (Exception e) {
            Assert.fail("Failed to send multipart email to an internal address.");
        }

        try {
            emailService.sendMultiPartEmail("Testing Subject", mp,
                    Collections.singleton("yintaosong@gmail.com"), settings);
        } catch (Exception e) {
            Assert.fail("Failed to send multipart email to an external address.");
        }
    }

}
