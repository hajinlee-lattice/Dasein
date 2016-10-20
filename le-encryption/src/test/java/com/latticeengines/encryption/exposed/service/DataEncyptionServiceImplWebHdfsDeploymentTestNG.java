package com.latticeengines.encryption.exposed.service;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.encryption.functionalframework.EncryptionTestNGBase;
import com.latticeengines.proxy.exposed.encryption.EncryptionWebHdfsProxy;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

public class DataEncyptionServiceImplWebHdfsDeploymentTestNG extends EncryptionTestNGBase {

    private static final Log log = LogFactory.getLog(DataEncyptionServiceImplWebHdfsDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/encryption/exposed/service";
    private static final String FILE_NAME = "test.txt";

    private CustomerSpace customerSpace;

    private String directory;

    @Value("${dataplatform.fs.web.defaultFS}")
    private String hdfsUrl;

    private String hdfsDir;
    private String updatedHdfsDir;
    private static final String BUILD_USER = System.getProperty("user.name");
    private static final String MKDIRS = "MKDIRS";
    private static final String GET_FILE_STATUS = "GETFILESTATUS";
    private static final String CREATE = "CREATE";
    private static final String RENAME = "RENAME";
    private static final String DELETE = "DELETE";
    private static final String OPEN = "OPEN";

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    private EncryptionWebHdfsProxy encryptionWebHdfsProxy;

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product, boolean encryptTenant)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        Map<String, Boolean> encryptionFeatureFlagMap = new HashMap<String, Boolean>();
        encryptionFeatureFlagMap.put(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName(), new Boolean(encryptTenant));
        testBed.bootstrapForProduct(product, encryptionFeatureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        log.info("Tenant id is " + mainTestTenant.getId());
        switchToSuperAdmin();
    }

    @Test(groups = "deployment", dataProvider = "provider", enabled = false)
    public void testCrud(Boolean enableEncryptionTenant) {
        log.info("Bootstrapping test tenants using tenant console with enableEncryptionTenant: "
                + enableEncryptionTenant.toString());
        try {
            setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, enableEncryptionTenant.booleanValue());
            if (enableEncryptionTenant.booleanValue()) {
                Assert.assertTrue(dataEncryptionService.isEncrypted(CustomerSpace.parse(mainTestTenant.getId())));
            } else {
                // Assert.assertFalse(dataEncryptionService.isEncrypted(CustomerSpace.parse(mainTestTenant.getId())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error creating the environment for the test.");
        }
        customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        directory = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), customerSpace).toString();
        System.out.println(String.format("Directory is %s", directory));
        hdfsDir = directory + "/testfolder";
        updatedHdfsDir = directory + "/testfolder_updated";
        testDirectoryCreation();
        testFileCreation();
        testFileReading();
        testUpdateDirectoryName();
        // testDeleteFile();
        // testDeleteDirectory();
    }

    protected void testDirectoryCreation() {
        log.info("Start creating a directory in Hdfs: " + hdfsDir);
        restTemplate.put(String.format("%s%s?user.name=%s&op=%s", hdfsUrl, hdfsDir, BUILD_USER, MKDIRS), String.class);
        // encryptionWebHdfsProxy.createDirectory(hdfsDir, BUILD_USER);
        readDirectory(hdfsDir);
    }

    @SuppressWarnings("rawtypes")
    protected void testFileCreation() {
        log.info("Start creating a file in Hdfs: " + hdfsDir);
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + FILE_NAME));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseEntity<ResponseDocument> response = restTemplate.exchange( //
                String.format("%s%s/%s?user.name=%s&op=%s", hdfsUrl, hdfsDir, FILE_NAME, BUILD_USER, CREATE),
                HttpMethod.PUT, //
                requestEntity, ResponseDocument.class);
        log.info("location is " + response.getHeaders().getLocation());
        String location = response.getHeaders().getLocation().toString();
        Assert.assertEquals(response.getStatusCode(), HttpStatus.TEMPORARY_REDIRECT);

        response = restTemplate.exchange(location, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        Assert.assertEquals(response.getStatusCode(), HttpStatus.CREATED);
    }

    protected void testFileReading() {
        log.info("Start reading the file created in Hdfs: " + hdfsDir);
        ResponseEntity<String> response = restTemplate.getForEntity(
                String.format("%s%s/%s?user.name=%s&op=%s", hdfsUrl, hdfsDir, FILE_NAME, BUILD_USER, OPEN),
                String.class);
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
        log.info("response is " + response.getBody());
        try {

            String fileContents = FileUtils.readFileToString(
                    new File(ClassLoader.getSystemResource(RESOURCE_BASE + "/" + FILE_NAME).getPath()));
            // Assert.assertEquals(fileContents, response);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading the file contents.");
        }
    }

    protected void testUpdateDirectoryName() {
        log.info("Start updating directory name in Hdfs...");
        restTemplate.put(String.format("%s%s?user.name=%s&op=%s&destination=%s", hdfsUrl, hdfsDir, BUILD_USER, RENAME,
                updatedHdfsDir), String.class);
        readDirectory(updatedHdfsDir);
    }

    protected void testDeleteFile() {
        log.info("Start deleting the file created in Hdfs: " + hdfsDir);
    }

    protected void testDeleteDirectory() {
        log.info("Start deleting directory in Hdfs...");
        restTemplate.delete(String.format("%s%s?user.name=%s&op=%s&destination=%s", hdfsUrl, updatedHdfsDir, BUILD_USER,
                DELETE, updatedHdfsDir), String.class);

        try {
            restTemplate.getForEntity(String.format("%s%s?op=%s", hdfsUrl, updatedHdfsDir, GET_FILE_STATUS),
                    String.class);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
            // pass
        }
    }

    protected void readDirectory(String hdfsDir) {
        log.info("Start reading directory in Hdfs: " + hdfsDir);
        ResponseEntity<String> response = restTemplate
                .getForEntity(String.format("%s%s?op=%s", hdfsUrl, hdfsDir, GET_FILE_STATUS), String.class);
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    }

    @DataProvider(name = "provider")
    private Object[][] getOptions() {
        return new Object[][] { new Object[] { Boolean.FALSE }, new Object[] { Boolean.TRUE } };
    }

}
