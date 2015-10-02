package com.latticeengines.eai.service.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.camel.CamelContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class DataExtractionServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    private String customer = "SFDC-Eai-Customer";

    @Autowired
    private ImportContext importContext;

    private String targetPath;

    private List<String> tableNameList = Arrays. <String>asList(new String[]{"Account", "Contact", "Lead", "Opportunity", "OpportunityContactRole"});

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath("Production", customer).toString());
        crmCredentialZKService.removeCredentials(customer, customer, true);
        targetPath = dataExtractionService.createTargetPath(customer);
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customer, customer, "defaultspaceId", spaceInfo);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010oIogZVEFGbL3n0qiAp6F66TC");
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        setupSalesforceImportConfig(customer);
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception{
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath("Production", customer).toString());
        crmCredentialZKService.removeCredentials(customer, customer, true);
    }

    @Test(groups = "functional")
    public void extractAndImport() throws Exception {

        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        dataExtractionService.extractAndImport(importConfig, importContext);

        Thread.sleep(30000L);
        checkDataExists(targetPath, tableNameList, 1);
        dataExtractionService.cleanUpTargetPathData(importContext);
        checkDataExists(targetPath, tableNameList, 0);
        checkExtractsDirectoryExists(customer, tableNameList);
    }

}
