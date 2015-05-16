package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.InstallVisiDBTemplateRequest;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmConfigService;

public class CrmConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmConfigService crmService;

    @Value("${pls.dataloader.rest.api}")
    private String dataLoaderUrl;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        String url = dataLoaderUrl + "/CreateDLTenant";

        try {
            afterClass();
        } catch (Exception ex) {
        }
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("tenantName", "tenantId");
        parameters.put("tenantAlias", "tenantId");
        parameters.put("ownerEmail", "richard.liu@lattice-engines.com");
        parameters.put("visiDBLocation", "ServerName=127.0.0.1");
        parameters.put("visiDBName", "tenantId");
        parameters.put("dmDeployment", "DMDeployment");
        parameters.put("contractExternalID", "tenantId");
        parameters.put("createNewVisiDB", "true");

        ((CrmConfigServiceImpl) crmService).excuteHttpRequest(url, parameters);

        url = dataLoaderUrl + "/InstallVisiDBStructureFile_Sync";
        uploadFile(url, "tenantId", "Template_MKTO.specs");

        url = dataLoaderUrl + "/InstallConfigFile_Sync";
        uploadFile(url, "tenantId", "Template_MKTO.config");

    }

    private void uploadFile(String url, String tenantId, String fileName) throws Exception {

        ClassPathResource resource = new ClassPathResource(fileName);
        String value = IOUtils.toString(resource.getURL());
        InstallVisiDBTemplateRequest request = new InstallVisiDBTemplateRequest(tenantId, value);

        String jsonStr = JsonUtils.serialize(request);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, getHeaders(), jsonStr);

        Assert.assertEquals(((CrmConfigServiceImpl) crmService).checkStatus(response), true);
    }

    public List<BasicNameValuePair> getHeaders() {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));
        return headers;
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        String url = dataLoaderUrl + "/DeleteDLTenant";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("tenantName", "tenantId");
        parameters.put("deleteVisiDBOption", "3");
        ((CrmConfigServiceImpl) crmService).excuteHttpRequest(url, parameters);
    }

    @Test(groups = "functional")
    public void config() {

        CrmCredential crmCredential = new CrmCredential();
        CrmConfig crmConfig = new CrmConfig();

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        crmConfig = new CrmConfig();
        crmConfig.setCrmCredential(crmCredential);
        crmService.config("marketo", "contractId.tenantId.spaceId", crmConfig);

    }

}
