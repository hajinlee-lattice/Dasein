package com.latticeengines.monitor.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@ContextConfiguration(locations = { "classpath:test-monitor-context.xml" })
public class EmailServiceImplTestNG extends AbstractTestNGSpringContextTests {// PowerMockTestCase

    private static final String PASSWORD = "password";
    private static final String HOSTPORT = "hostport";
    private static final String TENANT_NAME = "tenantName";
    private static final String MODEL_NAME = "modelName";

    @Inject
    private EmailService emailService;

    private Tenant tenant;
    private User user;
    @SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_FINAL")
    private Logger origLog;
    @SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_FINAL")
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
        emailService.sendNewUserEmail(user, PASSWORD, HOSTPORT, false);
    }

    @Test(groups = "functional")
    public void sendNewUserEmail() {
        emailService.sendNewUserEmail(user, PASSWORD, HOSTPORT, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new user email"));

        emailService.sendNewUserEmail(user, PASSWORD, HOSTPORT, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("new user email"));
    }

    @Test(groups = "functional")
    public void sendExistingUserEmail() {
        emailService.sendExistingUserEmail(tenant, user, HOSTPORT, true);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing user email"));

        emailService.sendExistingUserEmail(tenant, user, HOSTPORT, false);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("existing user email"));
    }

    @Test(groups = "functional")
    public void sendPlsForgetPasswordEmail() {
        emailService.sendPlsForgetPasswordEmail(user, PASSWORD, HOSTPORT);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("forget password"));
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
        emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") complete"));

        emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") complete"));
    }

    @Test(groups = "functional")
    public void sendPlsEnrichInternalAttributeErrorEmail() {
        emailService.sendPlsEnrichInternalAttributeErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") error"));

        emailService.sendPlsEnrichInternalAttributeErrorEmail(user, HOSTPORT, TENANT_NAME, MODEL_NAME, null);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("Sending PLS enrich internal attribute (" + MODEL_NAME + ") error"));
    }

    @Test(groups = "functional")
    public void sendPlsExportSegmentSuccessEmail() {
        emailService.sendPlsExportSegmentSuccessEmail(user, HOSTPORT, "export_id", "type", TENANT_NAME);

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("segment export complet"));

        emailService.sendPlsExportSegmentSuccessEmail(user, HOSTPORT, "export_id", "type", TENANT_NAME);

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
        emailService.sendPlsExportOrphanRecordsSuccessEmail(user, tenant.getName(), HOSTPORT, HOSTPORT, "export_id",
                "type");

        Mockito.verify(newLog, Mockito.times(0)).error(anyString());
        Assert.assertTrue(logs.get(0).contains("type export complet"));

        emailService.sendPlsExportOrphanRecordsSuccessEmail(user, tenant.getName(), HOSTPORT, HOSTPORT, "export_id",
                "type");

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

    /**
     * Test the email sent when an upload is complete.
     */
    @Test(groups = "functional")
    public void sendUploadCompleteEmail() {
        UploadEmailInfo uploadEmailInfo = new UploadEmailInfo();
        // List<String> recipientList = Arrays.asList(user.getEmail());
        List<String> recipientList = Arrays.asList(user.getEmail());
        uploadEmailInfo.setRecipientList(recipientList);
        uploadEmailInfo.setUploadDisplayName("Successful upload functional test");
        uploadEmailInfo.setSourceDisplayName("Source Display Name");
        uploadEmailInfo.setProjectDisplayName("Project Display Name");

        emailService.sendUploadCompletedEmail(uploadEmailInfo);
    }

    /**
     * Test the email send when an upload fails.
     */
    @Test(groups = "functional")
    public void sendUploadFailedEmail() {
        UploadEmailInfo uploadEmailInfo = new UploadEmailInfo();

        uploadEmailInfo.setRecipientList(Arrays.asList(user.getEmail()));
        uploadEmailInfo.setUploadDisplayName("Successful upload functional test");
        uploadEmailInfo.setSourceDisplayName("Source Display Name");
        uploadEmailInfo.setProjectDisplayName("Project Display Name");

        emailService.sendUploadFailedEmail(uploadEmailInfo);
    }

    /**
     * Test the email send when a new DCP user is added or assigned to a Tenant.
     */
    @Test(groups = "functional")
    public void sendDCPWelcomeEmail() {

        List<String> recipents = Arrays.asList(user.getEmail());

        recipents.forEach(emailAddr -> {
            IDaaSUser iDaasUser = new IDaaSUser();
            iDaasUser.setEmailAddress(emailAddr);
            iDaasUser.setFirstName("FirstName");
            String url = "https://connect-dev.dnb.com";
            emailService.sendDCPWelcomeEmail(iDaasUser, TENANT_NAME, url);
        });
    }

}
