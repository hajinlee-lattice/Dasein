package com.latticeengines.encryption.exposed.service;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.WebHdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.encryption.functionalframework.EncryptionTestNGBase;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public class DataEncryptionServiceImplWebHdfsDeploymentTestNG extends EncryptionTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataEncryptionServiceImplWebHdfsDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/encryption/exposed/service";
    private static final String FILE_NAME = "test.txt";

    private CustomerSpace customerSpace;

    private String directory;

    @Value("${hadoop.fs.web.webhdfs}")
    private String webHdfsUrl;

    private String hdfsDir;
    private String updatedHdfsDir;

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product, boolean encryptTenant)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        Map<String, Boolean> encryptionFeatureFlagMap = new HashMap<String, Boolean>();
        encryptionFeatureFlagMap.put(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName(), new Boolean(encryptTenant));
        mainTestTenant = testBed.bootstrapForProduct(product, encryptionFeatureFlagMap);
        log.info("Tenant id is " + mainTestTenant.getId());
        switchToSuperAdmin();
    }

    @Test(groups = "deployment", dataProvider = "provider", enabled = true)
    public void testCrud(Boolean enableEncryptionTenant) {
        log.info("Bootstrapping test tenants using tenant console with enableEncryptionTenant: "
                + enableEncryptionTenant.toString());
        try {
            setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, enableEncryptionTenant.booleanValue());
            if (enableEncryptionTenant.booleanValue()) {
                Assert.assertTrue(dataEncryptionService.isEncrypted(CustomerSpace.parse(mainTestTenant.getId())));
            } else {
                Assert.assertFalse(dataEncryptionService.isEncrypted(CustomerSpace.parse(mainTestTenant.getId())));
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
        testDeleteDirectory();
    }

    protected void testDirectoryCreation() {
        log.info("Start creating a directory in Hdfs: " + hdfsDir);
        try {
            WebHdfsUtils.mkdir(webHdfsUrl, yarnConfiguration, hdfsDir);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error creating the directory .");
        }
        readDirectory(hdfsDir);
    }

    protected void testFileCreation() {
        log.info("Start creating a file in Hdfs: " + hdfsDir);
        String fileContents;
        try {
            fileContents = FileUtils.readFileToString(
                    new File(ClassLoader.getSystemResource(RESOURCE_BASE + "/" + FILE_NAME).getPath()));
            WebHdfsUtils.writeToFile(webHdfsUrl, yarnConfiguration, String.format("%s/%s", hdfsDir, FILE_NAME),
                    fileContents);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error creating a file .");
        }

    }

    protected void testFileReading() {
        log.info("Start reading the file created in Hdfs: " + hdfsDir);
        try {
            FileStatus fileStatus = WebHdfsUtils.getFileStatus(webHdfsUrl, yarnConfiguration,
                    String.format("%s/%s", hdfsDir, FILE_NAME));
            Assert.assertTrue(fileStatus.isFile());
            String expectedFileContents = FileUtils.readFileToString(
                    new File(ClassLoader.getSystemResource(RESOURCE_BASE + "/" + FILE_NAME).getPath()));
            log.info("file contents are: " + WebHdfsUtils.getWebHdfsFileContents(webHdfsUrl, yarnConfiguration,
                    String.format("%s/%s", hdfsDir, FILE_NAME)));
            Assert.assertEquals(expectedFileContents, WebHdfsUtils.getWebHdfsFileContents(webHdfsUrl, yarnConfiguration,
                    String.format("%s/%s", hdfsDir, FILE_NAME)));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading the file contents.");
        }
    }

    protected void testUpdateDirectoryName() {
        log.info("Start updating directory name in Hdfs...");
        try {
            WebHdfsUtils.updateDirectoryName(webHdfsUrl, yarnConfiguration, hdfsDir, updatedHdfsDir);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error updating the directory name.");
        }
        readDirectory(updatedHdfsDir);
    }

    protected void testDeleteDirectory() {
        log.info("Start deleting directory in Hdfs...");
        try {
            WebHdfsUtils.rmdir(webHdfsUrl, yarnConfiguration, updatedHdfsDir);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error deleting the directory.");
        }
    }

    protected void readDirectory(String hdfsDir) {
        log.info("Start reading directory in Hdfs: " + hdfsDir);
        try {
            Assert.assertTrue(WebHdfsUtils.isDirectory(webHdfsUrl, yarnConfiguration, hdfsDir));
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading the directory .");
        }
    }

    @DataProvider(name = "provider")
    private Object[][] getOptions() {
        return new Object[][] { new Object[] { Boolean.FALSE }, new Object[] { Boolean.TRUE } };
    }

}
