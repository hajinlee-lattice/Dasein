package com.latticeengines.encryption.exposed.service;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.encryption.functionalframework.EncryptionTestNGBase;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

public class DataEncyptionServiceImplWebHdfsDeploymentTestNG extends EncryptionTestNGBase {

    private static final Log log = LogFactory.getLog(DataEncyptionServiceImplWebHdfsDeploymentTestNG.class);

    private CustomerSpace customerSpace;

    private String directory;

    @Value("${dataplatform.fs.web.defaultFS}")
    private String hdfsUrl;

    private String hdfsDir;
    private String updatedHdfsDir;
    private static final String BUILD_USER = System.getProperty("user.name");
    private static final String MKDIRS = "MKDIRS";
    private static final String GET_FILE_STATUS = "GETFILESTATUS";
    private static final String RENAME = "RENAME";
    private static final String DELETE = "DELETE";

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product, boolean encryptTenant)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        testBed.bootstrapForProduct(product, encryptTenant);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    @Test(groups = "deployment", dataProvider = "provider", enabled = true)
    public void testCrud(Boolean enableEncryptionTenant) {
        log.info("Bootstrapping test tenants using tenant console with enableEncryptionTenant: "
                + enableEncryptionTenant.toString());
        try {
            setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, enableEncryptionTenant.booleanValue());
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        directory = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), customerSpace).toString();
        System.out.println(String.format("Directory is %s", directory));
        hdfsDir = directory + "/testfolder";
        updatedHdfsDir = directory + "/testfolder_updated";
        testDirectoryCreation();
        testUpdateDirectoryName();
        testDeleteDirectory();
        cleanup();
    }

    protected void testDirectoryCreation() {
        log.info("Start creating a directory in Hdfs: " + hdfsDir);
        restTemplate.put(String.format("%s%s?user.name=%s&op=%s", hdfsUrl, hdfsDir, BUILD_USER, MKDIRS), String.class);
        readDirectory(hdfsDir);
    }

    protected void testUpdateDirectoryName() {
        log.info("Start updating directory name in Hdfs...");
        restTemplate.put(String.format("%s%s?user.name=%s&op=%s&destination=%s", hdfsUrl, hdfsDir, BUILD_USER, RENAME,
                updatedHdfsDir), String.class);
        readDirectory(updatedHdfsDir);
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

    protected void cleanup() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildPodPath(mainTestTenant.getId()).toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void readDirectory(String hdfsDir) {
        log.info("Start reading directory in Hdfs: " + hdfsDir);
        ResponseEntity<String> response = restTemplate
                .getForEntity(String.format("%s%s?op=%s", hdfsUrl, hdfsDir, GET_FILE_STATUS), String.class);
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    }

    @DataProvider(name = "provider")
    private Object[][] getOpetions() {
        return new Object[][] { new Object[] { Boolean.FALSE }, new Object[] { Boolean.TRUE } };
    }

}
