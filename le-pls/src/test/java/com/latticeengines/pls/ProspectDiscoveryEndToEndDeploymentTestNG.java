package com.latticeengines.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;

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
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private TenantService tenantService;

    private static String tenant;
    private static Tenant tenantToAttach;
    private CustomerSpace customerSpace;

    @BeforeClass(groups = "deployment.pd")
    public void setup() throws Exception {
        deleteAndCreateTwoTenants();
        setupTestEnvironment("pd");

        tenantToAttach = testingTenants.get(1);
        if (tenantToAttach.getName().contains("Tenant 1")) {
            tenantToAttach = testingTenants.get(0);
        }
        tenant = tenantToAttach.getId();
        customerSpace = CustomerSpace.parse(tenant);
        createImportTablesInMetadataStore(customerSpace);
        UserDocument doc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(doc);
    }

    private void createImportTablesInMetadataStore(CustomerSpace space) throws Exception {
        URL url = getClass().getClassLoader().getResource("Tables");
        File tablesDir = new File(url.getFile());
        File[] files = tablesDir.listFiles();

        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            String str = FileUtils.readFileToString(file);
            Table table = JsonUtils.deserialize(str, Table.class);

            table.setTenant(tenantToAttach);
            table.setTableType(TableType.IMPORTTABLE);

            DateTime date = new DateTime();
            table.getLastModifiedKey().setLastModifiedTimestamp(date.minusYears(2).getMillis());

            metadataProxy.createTable(space.toString(), table.getName(), table);
        }
    }

    private void deleteAndCreateTwoTenants() throws Exception {
        deleteFromCamille(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId + "PLSTenant1")
                .toString());
        deleteFromCamille(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId + "PLSTenant2")
                .toString());
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
            createTenantByRestCall(tenant);
        }
    }

    private void deleteFromCamille(String path) {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        try {
            camille.delete(new Path(String.format(path, podId)));
        } catch (Exception e) {
            log.warn("Path " + path + " doesn't exist.");
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

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TargetMarket.DEFAULT_NAME, TargetMarket.class);
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
        while (true) {
            WorkflowStatus status = workflowProxy.getWorkflowStatusFromApplicationId(applicationId);
            if (status == null) {
                continue;
            }
            if ((running && status.getStatus().isRunning()) || (!running && !status.getStatus().isRunning())) {
                return status;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
