package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.MetadataServlet;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.testframework.rest.StandaloneHttpServer;

public class SalesforceEaiServiceImplFunctionalTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    private String targetPath;

    private StandaloneHttpServer httpServer;

    private String customer = this.getClass().getSimpleName();

    private String customerSpace = CustomerSpace.parse(customer).toString();

    @Value("${eai.test.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.test.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.test.salesforce.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    private Tenant tenant;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Account", "Contact", "Lead",
            "Opportunity", "OpportunityContactRole" });

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanupCamilleAndHdfs(customer);

        initZK(customer);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        crmCredential.setUrl(productionLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        tenant = createTenant(customerSpace);
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
        }
        tenantService.registerTenant(tenant);

        List<Table> tables = getSalesforceTables(tableNameList);

        httpServer = new StandaloneHttpServer();
        httpServer.init(mockPort);
        httpServer.addServlet(new MetadataServlet(tables), "/metadata/customerspaces/" + customerSpace + "/*");
        httpServer.start();
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        cleanupCamilleAndHdfs(customer);
        httpServer.stop();
        tenantService.discardTenant(tenant);
    }

    @Test(groups = "functional", enabled = false)
    public void extractAndImport() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        targetPath = dataExtractionService.createTargetPath(customer) + "/"
                + importConfig.getSourceConfigurations().get(0).getSourceType().getName();
        importConfig.setProperty(ImportProperty.METADATAURL, mockMetadataUrl);
        ApplicationId appId = eaiService.extractAndImport(importConfig);

        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        checkDataExists(targetPath, tableNameList, 1);

    }
}
