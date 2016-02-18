package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.MarketoExtractAndImportUtil;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class MarketoEaiServiceImplDeploymentTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Activity", "Lead", "ActivityType" });

    private String customer = "Marketo-Eai";

    private String customerSpace = CustomerSpace.parse(customer).toString();

    private String targetPath;

    @Value("${eai.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.salesforce.password}")
    private String salesforcePasswd;

    private Tenant tenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        targetPath = dataExtractionService.createTargetPath(customer);
        HdfsUtils.rmdir(yarnConfiguration, targetPath);
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customer, customer, "defaultspaceId", spaceInfo);
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

        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNameList) {
            URL url = ClassLoader.getSystemResource(String.format(
                    "com/latticeengines/eai/service/impl/marketo/%s.avsc", tableName).toString());
            String str = FileUtils.readFileToString(new File(url.getFile()));
            Table table = JsonUtils.deserialize(str, Table.class);
            tables.add(table);
        }
        System.out.println(tables);
        eaiMetadataService.updateTables(customerSpace, tables);
    }

    @AfterClass(groups = "deployment")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void extractAndImport() throws Exception {
        SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
        Map<String, String> props = new HashMap<>();

        props.put(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        props.put(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        props.put(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");

        List<Table> tables = new ArrayList<>();
        Table activityType = MarketoExtractAndImportUtil.createMarketoActivityType();
        Table activity = MarketoExtractAndImportUtil.createMarketoActivity();
        tables.add(activityType);
        tables.add(activity);

        marketoImportConfig.setSourceType(SourceType.MARKETO);
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
        marketoImportConfig.setProperties(props);
        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setCustomerSpace(CustomerSpace.parse(customer));
        importConfig.addSourceConfiguration(marketoImportConfig);

        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        checkDataExists(targetPath, tableNameList, 1);
    }
}