package com.latticeengines.monitor.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;

@ContextConfiguration(locations = { "classpath:test-monitor-context.xml" })
public class EmailServiceImplTestNG extends AbstractTestNGSpringContextTests {//PowerMockTestCase

    private static final String PASSWORD = "password";
    private static final String HOSTPORT = "hostport";
    private static final String TENANT_NAME = "tenantName";
    private static final String MODEL_NAME = "modelName";

    @Inject
    private EmailService emailService;

    private Tenant tenant;
    private User user;
    private Logger origLog;
    private Logger newLog;
    private List<String> logs;

    @BeforeClass(groups = "functional")
    public void setup() {
        origLog = EmailServiceImpl.log;

        tenant = new Tenant();
        tenant.setName(TENANT_NAME);

        user = new User();
        user.setEmail("test_email@lattice-engines.com");
        user.setUsername("test_user_name");
        user.setFirstName("test_first_name");
        user.setLastName("test_last_name");

        newLog = Mockito.mock(Logger.class);
        EmailServiceImpl.log = newLog;

        Mockito.doAnswer(invocation -> {
                Object[] params = invocation.getArguments();
                logs.add((String) params[0]);
                return logs;
        }).when(newLog).info(any());
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        logs = new ArrayList<>();
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        EmailServiceImpl.log = origLog;
    }

    @Test(groups = "manual", enabled = false)
    public void sendPlsNewExternalUserEmailToGmail() {
        User user = new User();
        user.setFirstName("FirstName");
        user.setUsername("build.lattice.engines@gmail.com");
        user.setEmail("build.lattice.engines@gmail.com");
        emailService.sendPlsNewExternalUserEmail(user, PASSWORD, HOSTPORT, false);
    }

    @Test(groups = "functional")
    public void sendPlsNewInternalUserEmail() {
        emailService.sendPlsNewInternalUserEmail(tenant, user, PASSWORD, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new PLS internal user"));
    }

    @Test(groups = "functional")
    public void sendPlsNewExternalUserEmail() {
        emailService.sendPlsNewExternalUserEmail(user, PASSWORD, HOSTPORT, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new PLS external user"));

        emailService.sendPlsNewExternalUserEmail(user, PASSWORD, HOSTPORT, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new PLS external user"));
    }

    @Test(groups = "functional")
    public void sendPlsNewProspectingUserEmail() {
        emailService.sendPlsNewProspectingUserEmail(user, PASSWORD, null);
        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new PLS prospecting user"));
    }

    @Test(groups = "functional")
    public void sendPlsExistingInternalUserEmail() {
        emailService.sendPlsExistingInternalUserEmail(tenant, user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing PLS internal user"));
    }

    @Test(groups = "functional")
    public void sendPlsExistingExternalUserEmail() {
        emailService.sendPlsExistingExternalUserEmail(tenant, user, HOSTPORT, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing PLS external user"));

        emailService.sendPlsExistingExternalUserEmail(tenant, user, HOSTPORT, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing PLS external user"));
    }

    @Test(groups = "functional")
    public void sendPlsForgetPasswordEmail() {
        emailService.sendPlsForgetPasswordEmail(user, PASSWORD, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("forget password"));
    }

    @Test(groups = "functional")
    public void sendPdNewExternalUserEmail() {
        emailService.sendPdNewExternalUserEmail(user, PASSWORD, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new PD external user"));
    }

    @Test(groups = "functional")
    public void sendPdExistingExternalUserEmail() {
        emailService.sendPdExistingExternalUserEmail(tenant, user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing PD external user"));
    }

    @Test(groups = "functional")
    public void sendPlsImportDataSuccessEmail() {
        emailService.sendPlsImportDataSuccessEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS import data complete"));
    }

    @Test(groups = "functional")
    public void sendPlsImportDataErrorEmail() {
        emailService.sendPlsImportDataErrorEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS import data error"));
    }

    @Test(groups = "functional")
    public void sendPlsEnrichDataSuccessEmail() {
        emailService.sendPlsEnrichDataSuccessEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS enrichment data complete"));
    }

    @Test(groups = "functional")
    public void sendPlsEnrichDataErrorEmail() {
        emailService.sendPlsEnrichDataErrorEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS enrich data error"));
    }

    @Test(groups = "functional")
    public void sendPlsValidateMetadataSuccessEmail() {
        emailService.sendPlsValidateMetadataSuccessEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS validate metadata complete"));
    }

    @Test(groups = "functional")
    public void sendPlsMetadataMissingEmail() {
        emailService.sendPlsMetadataMissingEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS metadata missing"));
    }

    @Test(groups = "functional")
    public void sendPlsValidateMetadataErrorEmail() {
        emailService.sendPlsValidateMetadataErrorEmail(user, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS validate metadata error"));
    }

    @Test(groups = "functional")
    public void sendPlsCreateModelCompletionEmail() {
        emailService.sendPlsCreateModelCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS create model (" + MODEL_NAME + ") complete"));

        emailService.sendPlsCreateModelCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS create model (" + MODEL_NAME + ") complete"));
    }

    @Test(groups = "functional")
    public void sendPlsCreateModelErrorEmail() {
        emailService.sendPlsCreateModelErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS create model (" + MODEL_NAME + ") error"));

        emailService.sendPlsCreateModelErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS create model (" + MODEL_NAME + ") error"));
    }

    @Test(groups = "functional")
    public void sendPlsScoreCompletionEmail() {
        emailService.sendPlsScoreCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS scoring (" + MODEL_NAME + ") complete"));

        emailService.sendPlsScoreCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS scoring (" + MODEL_NAME + ") complete"));
    }

    @Test(groups = "functional")
    public void sendPlsScoreErrorEmail() {
        emailService.sendPlsScoreErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS scoring (" + MODEL_NAME + ") error"));

        emailService.sendPlsScoreErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS scoring (" + MODEL_NAME + ") error"));
    }

    @Test(groups = "functional")
    public void sendPlsOnetimeSfdcAccessTokenEmail() {
        emailService.sendPlsOnetimeSfdcAccessTokenEmail(user, "tenant_id", "access_token");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS one-time sfdc access token"));
    }

    @Test(groups = "functional", enabled = false)
    public void sendGlobalAuthForgetCredsEmail() {
        emailService.sendGlobalAuthForgetCredsEmail(user.getFirstName(), user.getLastName(), user.getUsername(),
                PASSWORD, user.getEmail(), null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("global auth forget creds"));
    }

    @Test(groups = "functional")
    public void sendPlsEnrichInternalAttributeCompletionEmail() {
        emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true,
                null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") complete"));

        emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false,
                null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") complete"));
    }

    @Test(groups = "functional")
    public void sendPlsEnrichInternalAttributeErrorEmail() {
        emailService.sendPlsEnrichInternalAttributeErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, true,
                null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") error"));

        emailService.sendPlsEnrichInternalAttributeErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, false,
                null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") error"));
    }

    @Test(groups = "functional")
    public void sendPlsExportSegmentSuccessEmail() {
        emailService.sendPlsExportSegmentSuccessEmail(user, HOSTPORT, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("segment export complet"));

        emailService.sendPlsExportSegmentSuccessEmail(user, HOSTPORT, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("segment export complete"));
    }

    @Test(groups = "functional")
    public void sendPlsExportSegmentErrorEmail() {
        emailService.sendPlsExportSegmentErrorEmail(user, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS export segment error"));

        emailService.sendPlsExportSegmentErrorEmail(user, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS export segment error"));
    }

    @Test(groups = "functional")
    public void sendPlsExportSegmentRunningEmail() {
        emailService.sendPlsExportSegmentRunningEmail(user, "export_id");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS export segment in-progress"));

        emailService.sendPlsExportSegmentRunningEmail(user, "export_id");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("PLS export segment in-progress"));
    }

    @Test(groups = "functional")
    public void sendPlsExportOrphanRunningEmail() {
        emailService.sendPlsExportOrphanRecordsRunningEmail(user, "export_id", "Orphan Contacts");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Orphan Contacts export in-progress"));

        emailService.sendPlsExportOrphanRecordsRunningEmail(user, "export_id", "Orphan Contacts");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Orphan Contacts export in-progress"));
    }

    @Test(groups = "functional")
    public void sendPlsExportOrphanSuccessEmail() {
        emailService.sendPlsExportOrphanRecordsSuccessEmail(user, HOSTPORT, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("type export complet"));

        emailService.sendPlsExportOrphanRecordsSuccessEmail(user, HOSTPORT, "export_id", "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("type export complete"));
    }

    @Test(groups = "functional")
    public void sendS3TemplateUpdateEmail() {
        S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();
        emailInfo.setUser("system@lattice-engines.com");
        emailInfo.setTemplateName("AccountSchema");
        emailInfo.setEntityType(EntityType.Accounts);
        emailInfo.setDropFolder("lattice-engines-dev/dropfolder/Templates/AccountSchema");
        emailService.sendS3TemplateUpdateEmail(user, tenant, HOSTPORT, emailInfo);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending s3 template update notification"));
    }

}
