package com.latticeengines.camille.lifecycle;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.lifecycle.PodLifecycleManager;

public class PodLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testCreate() throws Exception {
        String podId = "testPod";
        PodLifecycleManager.create(podId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
        PodLifecycleManager.create(podId);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String podId = "testPod";
        PodLifecycleManager.delete(podId);
        PodLifecycleManager.create(podId);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
        PodLifecycleManager.delete(podId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String podId = "testPod";
        Assert.assertFalse(PodLifecycleManager.exists(podId));
        PodLifecycleManager.create(podId);
        Assert.assertTrue(PodLifecycleManager.exists(podId));
        PodLifecycleManager.delete("testPod");
        Assert.assertFalse(PodLifecycleManager.exists(podId));
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<String> in = new HashSet<String>();
        for (int i = 0; i < 10; ++i) {
            String podId = Integer.toString(i);
            in.add(podId);
            PodLifecycleManager.create(podId);
        }

        Assert.assertTrue(PodLifecycleManager.getAll().containsAll(in));
    }
}
