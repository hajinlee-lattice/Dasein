package com.latticeengines.testframework.security.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.rest.LedpResponseErrorHandler;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml" })
public class GlobalAuthCleanupTestNG extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(GlobalAuthCleanupTestNG.class);
    private static final Long cleanupThreshold = TimeUnit.DAYS.toMillis(1);
    private static final String customerBase = "/user/s-analytics/customers";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Value("${admin.test.deployment.api:http://localhost:8085}")
    private String adminApiHostPort;

    private Camille camille;
    private String podId;
    private RestTemplate magicRestTemplate = new RestTemplate();
    private LedpResponseErrorHandler errorHandler = new LedpResponseErrorHandler();

    @BeforeClass(groups = "cleanup")
    public void setup() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
        if (adminApiHostPort.endsWith("/")) {
            adminApiHostPort = adminApiHostPort.substring(0, adminApiHostPort.lastIndexOf("/"));
        }
        MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
                Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        magicRestTemplate.setErrorHandler(errorHandler);
    }

    @Test(groups = "cleanup")
    public void cleanupTestTenants() throws Exception {
        List<Tenant> tenants = tenantService.getAllTenants();
        log.info("Scanning through " + tenants.size() + " tenants ...");
        for (Tenant tenant : tenants) {
            if (TestFrameworkUtils.isTestTenant(tenant)
                    && (System.currentTimeMillis() - tenant.getRegisteredTime()) > cleanupThreshold) {
                log.info("Found a test tenant to clean up: " + tenant.getId());
                cleanupTenantInGA(tenant);
                cleanupTenantInZK(tenant);
                cleanupTenantInHdfs(tenant);
                cleanupTenantsInDL(tenant);
            }
        }
        log.info("Finished cleaning up test tenants.");
    }

    private void cleanupTenantInGA(Tenant tenant) {
        log.info("Clean up tenant in GA: " + tenant.getId());
        tenantService.discardTenant(tenant);
    }

    private void cleanupTenantInZK(Tenant tenant) throws Exception {
        log.info("Clean up tenant in ZK: " + tenant.getId());
        String contractId = CustomerSpace.parse(tenant.getId()).getContractId();
        Path contractPath = PathBuilder.buildContractPath(podId, contractId);
        if (camille.exists(contractPath)) {
            camille.delete(contractPath);
        }
    }

    private void cleanupTenantInHdfs(Tenant tenant) throws Exception {
        log.info("Clean up tenant in HDFS: " + tenant.getId());
        String customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        String contractId = CustomerSpace.parse(tenant.getId()).getContractId();

        String contractPath = PathBuilder.buildContractPath(podId, contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }

        String customerPath = new Path(customerBase).append(customerSpace).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        }
        contractPath = new Path(customerBase).append(contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }
    }

    private void cleanupTenantsInDL(Tenant tenant) {
        log.info("Clean up test tenant " + tenant.getId() + " from DL.");
        CustomerSpace customerSpace =CustomerSpace.parse(tenant.getId());
        String tenantName = customerSpace.getTenantId();

        try {
            String permStoreUrl = adminApiHostPort + "/admin/internal/BODCDEVVINT207/BODCDEVVINT187/" + tenantName;
            magicRestTemplate.delete(permStoreUrl);
            log.info("Cleanup VDB permstore for tenant " + tenantName);
        } catch (Exception e) {
            log.error("Failed to clean up permstore for vdb " + tenantName + " : "
                    + errorHandler.getStatusCode() + ", " + errorHandler.getResponseString());
        }

        try {
            List<BasicNameValuePair> adHeaders = loginAd();
            String adminUrl = adminApiHostPort + "/admin/tenants/" + customerSpace.getTenantId() + "?contractId="
                    + customerSpace.getContractId();
            String response = HttpClientWithOptionalRetryUtils.sendGetRequest(adminUrl, false, adHeaders);
            TenantDocument tenantDoc = JsonUtils.deserialize(response, TenantDocument.class);
            String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
            DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenantName, "3");
            InstallResult result = dataLoaderService.deleteDLTenant(request, dlUrl, true);
            log.info("Delete DL tenant " + tenantName + " result=" + JsonUtils.serialize(result));
        } catch (Exception e) {
            log.error("Failed to clean up dl tenant " + tenantName + " : "
                    + errorHandler.getStatusCode() + ", " + errorHandler.getResponseString());
        }
    }


    private List<BasicNameValuePair> loginAd() throws IOException {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        Credentials credentials = new Credentials();
        credentials.setUsername(TestFrameworkUtils.AD_USERNAME);
        credentials.setPassword(TestFrameworkUtils.AD_PASSWORD);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(adminApiHostPort + "/admin/adlogin", false,
                headers, JsonUtils.serialize(credentials));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);
        String token = json.get("Token").asText();

        headers.add(new BasicNameValuePair("Authorization", token));
        return headers;
    }

}

