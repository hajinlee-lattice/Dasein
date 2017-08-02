package com.latticeengines.testframework.security.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml" })
public class GlobalAuthCleanupTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthCleanupTestNG.class);
    private static final Long cleanupThreshold = TimeUnit.DAYS.toMillis(7);
    private static final String customerBase = "/user/s-analytics/customers";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private RedshiftService redshiftService;

    @Value("${admin.test.deployment.api:http://localhost:8085}")
    private String adminApiHostPort;

    private Camille camille;
    private String podId;
    private RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
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
                cleanupTenantInZK(CustomerSpace.parse(tenant.getId()).getContractId());
                cleanupTenantInDL(CustomerSpace.parse(tenant.getId()).getContractId());
                deleteKey(tenant);
            }
        }

        cleanupTenantsInHdfs();
        cleanupZK();
        cleanupRedshift();

        log.info("Finished cleaning up test tenants.");
    }

    private void deleteKey(Tenant tenant) {
        try {
            HdfsUtils.deleteKey(yarnConfiguration, CustomerSpace.parse(tenant.getId()).getContractId());
        } catch (Exception e) {
            log.error(String.format("Failed to cleanup key for customer %s", tenant.getId()), e);
        }
    }

    private void cleanupTenantInGA(Tenant tenant) {
        if (!TestFrameworkUtils.isTestTenant(tenant)) {
            return;
        }

        log.info("Clean up tenant in GA: " + tenant.getId());
        tenantService.discardTenant(tenant);
    }

    private void cleanupZK() throws Exception {
        try {
            List<AbstractMap.SimpleEntry<Document, Path>> entries = camille.getChildren(PathBuilder.buildContractsPath(podId));
            if (entries != null) {
                for (AbstractMap.SimpleEntry<Document, Path> entry : entries) {
                    Path path = entry.getValue();
                    String contract = path.getSuffix();
                    if (TestFrameworkUtils.isTestTenant(contract)) {
                        long testTime = TestFrameworkUtils.getTestTimestamp(contract);
                        if (testTime > 0 && (System.currentTimeMillis() - testTime) > cleanupThreshold) {
                            cleanupTenantInZK(contract);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up test tenants in ZK.", e);
        }
    }

    private void cleanupTenantInZK(String contractId) throws Exception {
        if (!TestFrameworkUtils.isTestTenant(contractId)) {
            return;
        }

        log.info("Clean up tenant in ZK: " + contractId);
        Path contractPath = PathBuilder.buildContractPath(podId, contractId);
        if (camille.exists(contractPath)) {
            camille.delete(contractPath);
        }
    }

    private void cleanupTenantsInHdfs() {
        String contractsPath = PathBuilder.buildContractsPath(podId).toString();
        try {
            List<FileStatus> fileStatuses = HdfsUtils.getFileStatusesForDir(yarnConfiguration, contractsPath,
                    FileStatus::isDirectory);
            for (FileStatus fileStatus : fileStatuses) {
                if (TestFrameworkUtils.isTestTenant(fileStatus.getPath().getName())) {
                    Long modifiedTime = fileStatus.getModificationTime();
                    if ((System.currentTimeMillis() - modifiedTime) > cleanupThreshold) {
                        String contractId = fileStatus.getPath().getName();
                        log.info("Found an old test contract " + contractId);
                        try {
                            cleanupTenantInHdfs(contractId);
                            cleanupTenantInDL(contractId);
                            cleanupTenantInZK(contractId);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up test tenants in hdfs.", e);
        }
    }

    private void cleanupTenantInHdfs(String contractId) throws Exception {
        if (TestFrameworkUtils.isTestTenant(contractId)) {
            log.info("Clean up contract in HDFS: " + contractId);
            String customerSpace = CustomerSpace.parse(contractId).toString();
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
    }

    private void cleanupTenantInDL(String tenantName) {
        if (!TestFrameworkUtils.isTestTenant(tenantName)) {
            return;
        }

        log.info("Clean up test tenant " + tenantName + " from DL.");

        try {
            String permStoreUrl = adminApiHostPort + "/admin/internal/BODCDEVVINT207/BODCDEVVINT187/" + tenantName;
            magicRestTemplate.delete(permStoreUrl);
            log.info("Cleanup VDB permstore for tenant " + tenantName);
        } catch (Exception e) {
            log.error("Failed to clean up permstore for vdb " + tenantName + " : " + errorHandler.getStatusCode()
                    + ", " + errorHandler.getResponseString());
        }

        try {
            List<BasicNameValuePair> adHeaders = loginAd();
            String adminUrl = adminApiHostPort + "/admin/tenants/" + tenantName + "?contractId=" + tenantName;
            String response = HttpClientWithOptionalRetryUtils.sendGetRequest(adminUrl, false, adHeaders);
            TenantDocument tenantDoc = JsonUtils.deserialize(response, TenantDocument.class);
            String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
            DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenantName, "3");
            InstallResult result = dataLoaderService.deleteDLTenant(request, dlUrl, true);
            log.info("Delete DL tenant " + tenantName + " result=" + JsonUtils.serialize(result));
        } catch (Exception e) {
            log.error("Failed to clean up dl tenant " + tenantName + " : " + errorHandler.getStatusCode() + ", "
                    + errorHandler.getResponseString());
        }
    }

    private void cleanupRedshift() throws Exception {
        try {
            List<String> tables = redshiftService.getTables(TestFrameworkUtils.TENANTID_PREFIX);
            if (tables != null && !tables.isEmpty()) {
                log.info(String.format("Found %d test tenant tables in redshift.", tables.size()));
                for (String table: tables) {
                    String tenant = RedshiftUtils.extractTenantFromTableName(table);
                    if (TestFrameworkUtils.isTestTenant(tenant)) {
                        long testTime = TestFrameworkUtils.getTestTimestamp(tenant);
                        if (testTime > 0 && (System.currentTimeMillis() - testTime) > cleanupThreshold) {
                            log.info("Dropping redshift table " + table);
                            redshiftService.dropTable(table);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up test tenants in redshift.", e);
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
