package com.latticeengines.camille.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.PodLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.lifecycle.PodInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.PodProperties;

public class PodLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final PodInfo podInfo = new PodInfo(new PodProperties("PodLifecycleManagerUnitTestNG",
            "PodLifecycleManagerUnitTestNG"));

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
        PodLifecycleManager.create(podId, podInfo);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
        PodLifecycleManager.create(podId, podInfo);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        String podId = "testPod";
        PodLifecycleManager.delete(podId);
        PodLifecycleManager.create(podId, podInfo);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
        PodLifecycleManager.delete(podId);
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId)));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        String podId = "testPod";
        Assert.assertFalse(PodLifecycleManager.exists(podId));
        PodLifecycleManager.create(podId, podInfo);
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
            PodLifecycleManager.create(podId, podInfo);
        }

        List<AbstractMap.SimpleEntry<String, PodInfo>> all = PodLifecycleManager.getAll();
        List<String> allPods = new ArrayList<String>();
        for (AbstractMap.SimpleEntry<String, PodInfo> pair : all) {
            allPods.add(pair.getKey());
        }
        Assert.assertTrue(allPods.containsAll(in));
    }
}
