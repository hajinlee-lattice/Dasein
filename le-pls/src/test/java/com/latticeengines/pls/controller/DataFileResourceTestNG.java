package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.codec.digest.DigestUtils;
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
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

public class DataFileResourceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFileResourceTestNG.class);

    private Ticket ticket = null;

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementServiceImpl globalSessionManagementService;

    @Autowired
    private GlobalUserManagementServiceImpl globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "admin");
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "admin");

        setupDb(tenant1, tenant2);

        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/TENANT1");
        String dir = modelingServiceHdfsBaseDir
                + "/TENANT1/models/Q_PLS_Modeling_TENANT1/8195dcf1-0898-4ad3-b94d-0d0f806e979e/1423547416066_0001/";
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");

        HdfsUtils.mkdir(yarnConfiguration, dir);
        HdfsUtils.mkdir(yarnConfiguration, dir + "/enhancements");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/enhancements/modelsummary.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_model.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_readoutsample.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_scored.txt");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/test_explorer.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), dir + "/rf_model.txt");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dataProvider = "dataFileProvider")
    public void dataFileResource(String fileType, final String mimeType) {
        UserDocument doc = loginAndAttach("admin");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
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
                { "predictorcsv", "application/csv" }, //
                { "readoutcsv", "application/csv" }, //
                { "scorecsv", "text/plain" }, //
                { "explorercsv", "application/csv" }, //
                { "rfmodelcsv", "text/plain" }

        };
    }
}
