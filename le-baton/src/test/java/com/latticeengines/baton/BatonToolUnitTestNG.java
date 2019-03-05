package com.latticeengines.baton;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class BatonToolUnitTestNG {

    private BatonService service;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        service = new BatonServiceImpl();
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

        service.createTenant(contractId, tenantId, spaceId, new CustomerSpaceInfo(new CustomerSpaceProperties(), ""));
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

        service.loadDirectory(tempDir.toString(), "testDir");
        String podId = CamilleEnvironment.getPodId();

        Assert.assertEquals(c.get(new Path(String.format("/Pods/%s/testDir/0/0.txt", podId))).getData(), "zero");
        Assert.assertEquals(c.get(new Path(String.format("/Pods/%s/testDir/0/1/1.txt", podId))).getData(), "one");
    }

    @Test(groups = "unit")
    public void testLoadDirectoryRootPod() throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        File tempDir = Files.createTempDir();
        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");


        service.loadDirectory(tempDir.toString(), "");
        String podId = CamilleEnvironment.getPodId();

        Assert.assertEquals(c.get(new Path(String.format("/Pods/%s/0/0.txt", podId))).getData(), "zero");
        Assert.assertEquals(c.get(new Path(String.format("/Pods/%s/0/1/1.txt", podId))).getData(), "one");
    }

    @Test(groups = "unit")
    public void testLoadDirecotoryByDirectory() {
        DocumentDirectory sourceDir = new DocumentDirectory(new Path("/whatever"));
        sourceDir.add("/prop", "");
        sourceDir.add("/prop/prop1", "1.23");
        sourceDir.add("/prop/prop2", "1.23");
        sourceDir.add("/prop2", "value2");
        sourceDir.add("/prop2/prop1", "value2");

        Camille c = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();

        Path rootPath = PathBuilder.buildPodPath(podId).append(new Path("/root"));
        service.loadDirectory(sourceDir, rootPath);

        DocumentDirectory storedDir = c.getDirectory(rootPath);

        // change to the same root path before compare two directories
        sourceDir.makePathsLocal();
        storedDir.makePathsLocal();

        Assert.assertEquals(sourceDir, storedDir);
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
