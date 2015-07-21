package com.latticeengines.pls.controller;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class DataFileResourceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFileResourceTestNG.class);
    private static final String TENANT_ID = "TENANT1";
    private static final String UUID = "8195dcf1-0898-4ad3-b94d-0d0f806e979e";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        setupUsers();

        Tenant tenant1 = new Tenant();
        tenant1.setId(TENANT_ID);
        tenant1.setName(TENANT_ID);
        tenantService.discardTenant(tenant1);
        tenantService.registerTenant(tenant1);
        userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, TENANT_ID, SUPER_ADMIN_USERNAME);

        setupDbWithEloquaSMB(TENANT_ID, TENANT_ID);

        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + mainTestingTenant.getId());
        String dir = modelingServiceHdfsBaseDir
                + "/" + TENANT_ID + "/models/Q_PLS_Modeling_TENANT1/" + UUID + "/1423547416066_0001/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/diagnostics.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/metadata.avsc");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void teardown() throws Exception {
        userService.resignAccessLevel(TENANT_ID, SUPER_ADMIN_USERNAME);
        Tenant tenant1 = tenantService.findByTenantId(TENANT_ID);
        tenantService.discardTenant(tenant1);
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + TENANT_ID);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dataProvider = "dataFileProvider")
    public void dataFileResource(String fileType, final String mimeType) {
        Tenant tenantToAttach = tenantService.findByTenantId(TENANT_ID);
        UserDocument uDoc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(uDoc);
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        Assert.assertNotNull(response);
        Assert.assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);

        String modelId = map.get("Id");
        restTemplate.execute(getRestAPIHostPort() + "/pls/datafiles/" + fileType + "/" + modelId, HttpMethod.GET,
                new RequestCallback() {
                    @Override
                    public void doWithRequest(ClientHttpRequest request) throws IOException {
                    }
                }, new ResponseExtractor<Map<String, String>>() {
                    @Override
                    public Map<String, String> extractData(ClientHttpResponse response) throws IOException {
                        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
                        HttpHeaders headers = response.getHeaders();
                        Assert.assertTrue(headers.containsKey("Content-Disposition"));
                        Assert.assertTrue(headers.containsKey("Content-Type"));
                        Assert.assertEquals(headers.getFirst("Content-Type"), mimeType);
                        Assert.assertTrue(IOUtils.readLines(response.getBody()).size() > 0);
                        response.close();
                        return Collections.emptyMap();
                    }
                });
    }

    @DataProvider(name = "dataFileProvider")
    public static Object[][] getDataFileProvider() {
        return new Object[][] { { "modeljson", "application/json" }, //
                { "diagnosticsjson", "application/json" }, //
                { "metadataavsc", "application/json" }, //
                { "predictorcsv", "application/csv" }, //
                { "readoutcsv", "application/csv" }, //
                { "scorecsv", "text/plain" }, //
                { "explorercsv", "application/csv" }, //
                { "rfmodelcsv", "text/plain" }

        };
    }
}
