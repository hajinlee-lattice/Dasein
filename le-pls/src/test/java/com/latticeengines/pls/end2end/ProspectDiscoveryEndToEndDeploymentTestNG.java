package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;

public class ProspectDiscoveryEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(ProspectDiscoveryEndToEndDeploymentTestNG.class);

    private static final String PLS_TARGETMARKET_URL = "pls/targetmarkets/";

    @Value("${pls.test.sfdc.user.name}")
    private String salesforceUserName;

    @Value("${pls.test.sfdc.passwd.encrypted}")
    private String salesforcePasswd;

    @Value("${pls.test.sfdc.securitytoken}")
    private String salesforceSecurityToken;

    @Autowired
    private WorkflowProxy workflowProxy;

    private static String tenant;
    private static Tenant tenantToAttach;
    private CustomerSpace customerSpace;

    @BeforeClass(groups = "deployment.pd")
    public void setup() throws Exception {
        System.out.println("Deleting existing test tenants ...");
        deleteTwoTenants();

        System.out.println("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironment("pd", true);

        System.out.println("Setting up testing users ...");
        tenantToAttach = testingTenants.get(1);
        if (tenantToAttach.getName().contains("Tenant 1")) {
            tenantToAttach = testingTenants.get(0);
        }
        tenant = tenantToAttach.getId();
        customerSpace = CustomerSpace.parse(tenant);
        UserDocument doc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(doc);

        log.info("Test environment setup finished.");
        System.out.println("Test environment setup finished.");
    }

    private void deleteTwoTenants() throws Exception {
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
        }
    }

    @Test(groups = "deployment.pd")
    public void validateSfdcCreds() {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" + customerSpace.toString() + "&isProduction=true&verifyOnly=true",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000ZhOVEA0");
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment.pd", dependsOnMethods = { "validateSfdcCreds" }, enabled = true)
    public void createDefaultTargetMarket() throws Exception {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL + "default", null, TargetMarket.class);

        TargetMarket targetMarket = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_TARGETMARKET_URL + TargetMarket.DEFAULT_NAME, TargetMarket.class);
        assertTrue(targetMarket.getIsDefault());

        System.out.println("Workflow app id = " + targetMarket.getApplicationId());
        waitForWorkflowStatus(targetMarket.getApplicationId(), true);

        boolean exception = false;
        try {
            restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL + "default", null, Map.class);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);

        WorkflowStatus completedStatus = waitForWorkflowStatus(targetMarket.getApplicationId(), false);
        assertEquals(completedStatus.getStatus().name(), BatchStatus.COMPLETED.name());

        List<?> reports = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports", List.class);

        assertTrue(reports.size() > 0);
        for (Object r : reports) {
            Map<String, String> map = (Map<String, String>) r;
            Report report = restTemplate.getForObject(
                    String.format("%s/pls/reports/%s", getRestAPIHostPort(), map.get("name")), Report.class);
            String reportContent = new String(report.getJson().getPayload());
            System.out.println(String.format("Report %s:%s with payload:\n%s\n", report.getName(), report.getPurpose(),
                    reportContent));
        }
    }

    private WorkflowStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        WorkflowStatus status = null;

        while (true) {
            try {
                status = workflowProxy.getWorkflowStatusFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow status exception: %s", e.getMessage()));

                status = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((status != null) &&
               ((running && status.getStatus().isRunning()) ||
                (!running && !status.getStatus().isRunning()))) {
                return status;
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
