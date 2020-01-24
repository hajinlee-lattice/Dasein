package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.CrmConfigService;
import com.latticeengines.remote.exposed.service.DataLoaderService;
public class CrmConfigServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Inject
    private CrmConfigService crmService;

    @Inject
    private DataLoaderService dataLoaderService;

    @Value("${pls.dataloader.rest.api}")
    private String dataLoaderUrl;

    private final String tenant = "PLSCrmConfigTestTenant";

    private static final int SUCCESS = 3;

    @BeforeClass(groups = { "functional" }, enabled = false)
    public void setup() throws Exception {
        try {
            afterClass();
        } catch (Exception ignore) {
            // ignoring any error from test env cleanup
        }

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

    @AfterClass(groups = { "functional" }, enabled = false)
    public void afterClass() throws Exception {
        String url = dataLoaderUrl + "/DeleteDLTenant";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("tenantName", tenant);
        parameters.put("deleteVisiDBOption", "3");
        excuteHttpRequest(url, parameters);
    }

    @Test(groups = "functional", enabled = false)
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
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonObject = jsonParser.readTree(status);
        JsonNode statusCode = jsonObject.get("Status");
        if (statusCode != null && statusCode.asLong() == 3L) {
            return true;
        }
        JsonNode isSuccessful = jsonObject.get("Success");
        if (isSuccessful != null && isSuccessful.asBoolean()) {
            return true;
        }
        String errorMsg = jsonObject.get("ErrorMessage").asText();
        throw new RuntimeException(errorMsg);
    }
}
