package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.MetadataServlet;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class SalesforceEaiServiceImplFunctionalTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private ImportContext importContext;

    private String targetPath;

    private StandaloneHttpServer httpServer;

    private static final int PORT = 9002;

    private String customer = this.getClass().getSimpleName();

    private String customerSpace = CustomerSpace.parse(customer).toString();

    @Value("${eai.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.salesforce.password}")
    private String salesforcePasswd;

    private Tenant tenant;

    private List<Table> tables;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Account", "Contact", "Lead",
            "Opportunity", "OpportunityContactRole" });

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        targetPath = dataExtractionService.createTargetPath(customer);
        HdfsUtils.rmdir(yarnConfiguration, targetPath);

        initZK(customer);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        tenant = createTenant(customerSpace);
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
        }
        tenantService.registerTenant(tenant);

        tables = new ArrayList<>();
        for (String tableName : tableNameList) {
            URL url = ClassLoader.getSystemResource(String.format(
                    "com/latticeengines/eai/service/impl/salesforce/%s.avsc", tableName).toString());
            String str = FileUtils.readFileToString(new File(url.getFile()));
            Table table = JsonUtils.deserialize(str, Table.class);
            tables.add(table);
        }
        System.out.println(tables);

        httpServer = new StandaloneHttpServer();
        httpServer.init(PORT);
        httpServer.addServlet(new MetadataServlet(tables), "/metadata/customerspaces/" + customerSpace + "/tables/*");
        httpServer.start();
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        httpServer.stop();
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
        tenantService.discardTenant(tenant);

    }

    @Test(groups = "functional")
    public void extractAndImport() throws Exception {
        setupSalesforceImportConfig(customer);
        ApplicationId appId = eaiService.extractAndImport(importConfig);

        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        checkDataExists(targetPath, tableNameList, 1);

    }
}
