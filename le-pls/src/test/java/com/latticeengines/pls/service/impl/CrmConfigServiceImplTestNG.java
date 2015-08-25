package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmConfigService;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class CrmConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private CrmConfigService crmService;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Value("${pls.dataloader.rest.api}")
    private String dataLoaderUrl;

    private final String tenant = "PLSCrmConfigTestTenant";

    private final static int SUCCESS = 3;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        String url = dataLoaderUrl + "/CreateDLTenant";

        try {
            afterClass();
        } catch (Exception ex) {
            // ignore
        }
//        Map<String, Object> parameters = new HashMap<>();
//        parameters.put("tenantName", tenant);
//        parameters.put("tenantAlias", tenant);
//        parameters.put("ownerEmail", "richard.liu@lattice-engines.com");
//        parameters.put("visiDBLocation", "ServerName=127.0.0.1");
//        parameters.put("visiDBName", tenant);
//        parameters.put("dmDeployment", tenant);
//        parameters.put("contractExternalID", tenant);
//        parameters.put("createNewVisiDB", "true");
//
//        excuteHttpRequest(url, parameters);

        CreateVisiDBDLRequest.Builder builder = new CreateVisiDBDLRequest.Builder(tenant, tenant, tenant);
        builder.ownerEmail("richard.liu@lattice-engines.com").visiDBLocation("ServerName=127.0.0.1").visiDBName(tenant).createNewVisiDB(true);
        CreateVisiDBDLRequest createRequest = builder.build();
        InstallResult response = dataLoaderService.createDLTenant(createRequest, dataLoaderUrl);
        Assert.assertEquals(response.getStatus(), SUCCESS, response.getErrorMessage());

        ClassPathResource resource = new ClassPathResource("Template_MKTO.specs");
        String value = IOUtils.toString(resource.getURL());
        InstallTemplateRequest request = new InstallTemplateRequest(tenant, value);
        response = dataLoaderService.installVisiDBStructureFile(request, dataLoaderUrl);
        Assert.assertEquals(response.getStatus(), SUCCESS, response.getErrorMessage());


        resource = new ClassPathResource("Template_MKTO.config");
        value = IOUtils.toString(resource.getURL());
        request = new InstallTemplateRequest(tenant, value);
        response = dataLoaderService.installDataLoaderConfigFile(request, dataLoaderUrl);
        Assert.assertEquals(response.getStatus(), SUCCESS, response.getErrorMessage());
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
        parameters.put("tenantName", tenant);
        parameters.put("deleteVisiDBOption", "3");
        excuteHttpRequest(url, parameters);
    }

    @Test(groups = "functional")
    public void config() {
        CrmCredential crmCredential;
        CrmConfig crmConfig;

        // marketo
        crmCredential = new CrmCredential();
        crmCredential.setUserName("latticeenginessandbox1_9026948050BD016F376AE6");
        crmCredential.setPassword("41802295835604145500BBDD0011770133777863CA58");
        crmConfig = new CrmConfig();
        crmConfig.setCrmCredential(crmCredential);
        crmService.config("marketo", "PLSTestContract." + tenant + ".Production", crmConfig);
        
        //sfdc
        crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        crmConfig = new CrmConfig();
        crmConfig.setCrmCredential(crmCredential);
        crmService.config("sfdc", "PLSTestContract." + tenant + ".Production", crmConfig);

    }

    void excuteHttpRequest(String url, Map<String, Object> parameters) {
        try {
            String jsonStr = JsonUtils.serialize(parameters);
            String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, getHeaders(), jsonStr);
            checkStatus(response);
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18035, ex, new String[] { ex.getMessage() });
        }
    }

    boolean checkStatus(String status) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(status);
        Long statusCode = (Long) jsonObject.get("Status");
        if (statusCode != null && statusCode == 3L) {
            return true;
        }
        Boolean isSuccessful = (Boolean) jsonObject.get("Success");
        if (isSuccessful != null && isSuccessful) {
            return true;
        }
        String errorMsg = (String) jsonObject.get("ErrorMessage");
        throw new RuntimeException(errorMsg);
    }
}
