package com.latticeengines.pls;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.security.exposed.AccessLevel;

/**
 * @author rgonzalez
 *
 */
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
    private CrmCredentialZKService crmCredentialZKService;
    
    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    private static String tenant;
    private static Tenant tenantToAttach;
    private CustomerSpace customerSpace;

    @BeforeClass(groups = "deployment.pd", enabled = false)
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
        deleteFromCamille(String.format("/Pods/%s/Contracts/%sPLSTenant1", contractId));
        deleteFromCamille(String.format("/Pods/%s/Contracts/%sPLSTenant2", contractId));
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant: testingTenants) {
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
                + "/pls/credentials/sfdc?tenantId=" +  customerSpace.toString() + "&isProduction=true&verifyOnly=true",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000ZhOVEA0");
    }
    
    @Test(groups = "deployment.pd", dependsOnMethods = { "validateSfdcCreds" }, enabled = true)
    public void createDefaultTargetMarket() throws Exception {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL + "default", null, Void.class);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TargetMarket.DEFAULT_NAME, TargetMarket.class);
        assertTrue(targetMarket.getIsDefault());

        //waitForWorkflowCompletion(targetMarket.getApplicationId());
    }

    private WorkflowStatus waitForWorkflowCompletion(String applicationId) {
        while (true) {
            WorkflowStatus status = workflowProxy.getWorkflowStatusFromApplicationId(applicationId);
            if (!status.getStatus().isRunning()) {
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
