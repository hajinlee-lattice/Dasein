package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.AppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.appmaster.service.AppMasterServiceResponse;
import com.latticeengines.eai.appmaster.service.AppmasterServiceRequest;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class SalesforceEaiServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private AppmasterServiceClient appmasterServiceClient;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Account", "Contact", "Lead",
            "Opportunity", "OpportunityContactRole" });

    private String customer = "Salesforce-Eai";

    private String targetPath;

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
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
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010oIogZVEFGbL3n0qiAp6F66TC");
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath("Production", customer).toString());
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
    }

    @Test(groups = { "functional", "functional.production" }, enabled = true)
    public void extractAndImport() throws Exception {
        setupSalesforceImportConfig(customer);
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        BaseObject request = new AppmasterServiceRequest();
        Thread.sleep(20000);
        BaseResponseObject response = ((MindAppmasterServiceClient) appmasterServiceClient).doMindRequest(request);
        log.info(((AppMasterServiceResponse) response));

        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        checkDataExists(targetPath, tableNameList, 1);
    }
}
