package com.latticeengines.security.exposed.service.impl;

import java.util.Collections;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.EmailSettings;

import junit.framework.Assert;

public class EmailServiceImplUnitTestNG {

    private EmailServiceImpl emailService = new EmailServiceImpl();
    
    @Test(groups = "unit")
    public void testSendEmail() {
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

}
