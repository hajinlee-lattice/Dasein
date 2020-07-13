package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationType;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class JobLevelEmailNotificationDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private EmailProxy emailProxy;

    @Inject
    private TenantService tenantService;

    private static final String email = "pls-super-admin-tester@lattice-engines.com";

    private String tenantId;
    private Tenant tenant;

    private S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();
    private AdditionalEmailInfo additionalEmailInfo = new AdditionalEmailInfo();

    private static final Logger log = LoggerFactory.getLogger(JobLevelEmailNotificationDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        tenant = mainTestTenant;
        tenantId = tenant.getId();

        initJobLevels();
        initEmailInfo();
    }

    @AfterClass(groups = { "deployment" })
    public void tearDown() {
    }

    @Test(groups = "deployment", enabled = true)
    public void testSendCDLProcessAnalyzeEmail() {
        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.ERROR, "CDLProcessAnalyze");
        Assert.assertTrue(emailProxy.sendCDLProcessAnalyzeEmail("FAILED", tenantId, additionalEmailInfo));
        Assert.assertFalse(emailProxy.sendCDLProcessAnalyzeEmail("COMPLETED", tenantId, additionalEmailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.INFO, "CDLProcessAnalyze");
        Assert.assertTrue(emailProxy.sendCDLProcessAnalyzeEmail("COMPLETED", tenantId, additionalEmailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.ERROR, "CDLProcessAnalyze");
        Assert.assertFalse(emailProxy.sendCDLProcessAnalyzeEmail("COMPLETED", tenantId, additionalEmailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.INFO, "CDLProcessAnalyze");
        Assert.assertTrue(emailProxy.sendCDLProcessAnalyzeEmail("COMPLETED", tenantId, additionalEmailInfo));
    }

    @Test(groups = "deployment", enabled = true)
    public void testSendS3ImportEmail() {
        tenant.setNotificationType(TenantEmailNotificationType.SINGLE_USER);
        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.ERROR, "S3Import");
        sendS3ImportEmails();
        Assert.assertTrue(emailProxy.sendS3ImportEmail("FAILED", tenantId, emailInfo));
        Assert.assertFalse(emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo));
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.ERROR, "S3Import");
        Assert.assertFalse(emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo));
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.INFO, "S3Import");
        Assert.assertTrue(emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo));
        Assert.assertTrue(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.INFO, "S3Import");
        Assert.assertTrue(emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo));
        Assert.assertTrue(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.WARNING, TenantEmailNotificationLevel.ERROR, "S3Import");
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.WARNING, "S3Import");
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.WARNING, TenantEmailNotificationLevel.WARNING, "S3Import");
        Assert.assertFalse(emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo));
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));


        emailInfo.setErrorMsg("test error msg");
        Assert.assertTrue(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.WARNING, TenantEmailNotificationLevel.ERROR, "S3Import");
        Assert.assertFalse(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.WARNING, "S3Import");
        Assert.assertTrue(emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo));

    }

    @Test(groups = "deployment", enabled = true)
    public void testSendS3TemplateCreateEmail() {
        tenant.setNotificationType(TenantEmailNotificationType.SINGLE_USER);
        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.ERROR, "S3TemplateCreate");
        Assert.assertFalse(emailProxy.sendS3TemplateCreateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.INFO, "S3TemplateCreate");
        Assert.assertTrue(emailProxy.sendS3TemplateCreateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.ERROR, "S3TemplateCreate");
        Assert.assertFalse(emailProxy.sendS3TemplateCreateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.INFO, "S3TemplateCreate");
        Assert.assertTrue(emailProxy.sendS3TemplateCreateEmail(tenantId, emailInfo));
    }

    @Test(groups = "deployment", enabled = true)
    public void testSendS3TemplateUpdateEmail() {
        tenant.setNotificationType(TenantEmailNotificationType.SINGLE_USER);
        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.ERROR, "S3TemplateUpdate");
        Assert.assertFalse(emailProxy.sendS3TemplateUpdateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.ERROR, TenantEmailNotificationLevel.INFO, "S3TemplateUpdate");
        Assert.assertTrue(emailProxy.sendS3TemplateUpdateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.ERROR, "S3TemplateUpdate");
        Assert.assertFalse(emailProxy.sendS3TemplateUpdateEmail(tenantId, emailInfo));

        setLevels(TenantEmailNotificationLevel.INFO, TenantEmailNotificationLevel.INFO, "S3TemplateUpdate");
        Assert.assertTrue(emailProxy.sendS3TemplateUpdateEmail(tenantId, emailInfo));
    }

    private void setLevels(TenantEmailNotificationLevel tenantLevel, TenantEmailNotificationLevel jobLevel,
            String jobType) {
        tenant.setNotificationLevel(tenantLevel);
        switch (jobType) {
        case "S3TemplateCreate":
            tenant.putJobLevel("S3TemplateCreate", jobLevel);
            break;
        case "S3TemplateUpdate":
            tenant.putJobLevel("S3TemplateUpdate", jobLevel);
            break;
        case "S3Import":
            tenant.putJobLevel("S3Import", jobLevel);
            break;
        case "CDLProcessAnalyze":
            tenant.putJobLevel("CDLProcessAnalyze", jobLevel);
            break;
        default:
        }
        tenantService.updateTenant(tenant);
    }

    private void sendS3ImportEmails() {
        emailProxy.sendS3ImportEmail("FAILED", tenantId, emailInfo);
        emailProxy.sendS3ImportEmail("SUCCESS", tenantId, emailInfo);
        emailProxy.sendS3ImportEmail("IN_PROGRESS", tenantId, emailInfo);
    }

    private void initEmailInfo() {
        emailInfo.setUser(email);
        emailInfo.setTenantName(tenant.getName());
        emailInfo.setTemplateName("TestTemplate");
        emailInfo.setEntityType(EntityType.Accounts);
        emailInfo.setDropFolder("lattice-engines-dev/dropfolder/Templates/AccountSchema");

        additionalEmailInfo.setUserId("pls-super-admin-tester@lattice-engines.com");
    }

    private void initJobLevels() {
        tenant.putJobLevel("S3TemplateCreate", TenantEmailNotificationLevel.ERROR);
        tenant.putJobLevel("S3TemplateUpdate", TenantEmailNotificationLevel.ERROR);
        tenant.putJobLevel("S3Import", TenantEmailNotificationLevel.ERROR);
        tenant.putJobLevel("CDLProcessAnalyze", TenantEmailNotificationLevel.ERROR);
    }

}
