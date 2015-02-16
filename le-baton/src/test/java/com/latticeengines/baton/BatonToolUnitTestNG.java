package com.latticeengines.baton;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.Path;

public class BatonToolUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreateTenant() throws Exception {
        String contractId = "testContractId";
        String tenantId = "testTenantId";
        String spaceId = "testSpaceId";

        BatonTool.createTenant(contractId, tenantId, spaceId);
        Assert.assertTrue(TenantLifecycleManager.exists(contractId, tenantId));
    }

    @Test(groups = "unit")
    public void testLoadDirectory() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        File tempDir = Files.createTempDir();
        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");

        BatonTool.loadDirectory(tempDir.toString(), "testDir");
        String podId = CamilleEnvironment.getPodId();

        Assert.assertTrue(c.get(new Path(String.format("/Pods/%s/testDir/0/0.txt", podId))).getData().equals("zero"));
        Assert.assertTrue(c.get(new Path(String.format("/Pods/%s/testDir/0/1/1.txt", podId))).getData().equals("one"));
    }

    @Test(groups = "unit")
    public void testLoadDirectoryRootPod() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        File tempDir = Files.createTempDir();
        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");

        BatonTool.loadDirectory(tempDir.toString(), "");
        String podId = CamilleEnvironment.getPodId();

        Assert.assertTrue(c.get(new Path(String.format("/Pods/%s/0/0.txt", podId))).getData().equals("zero"));
        Assert.assertTrue(c.get(new Path(String.format("/Pods/%s/0/1/1.txt", podId))).getData().equals("one"));
    }

    private static void createDirectory(String path) {
        File dir = new File(path);
        dir.mkdir();
        dir.deleteOnExit();
    }

    private static void createTextFile(String path, String contents) throws FileNotFoundException {
        try (PrintWriter w = new PrintWriter(path)) {
            w.print(contents);
        }
        new File(path).deleteOnExit();
    }
}