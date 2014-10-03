package com.latticeengines.camille.lifecycle;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.CamilleTestEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Pod;

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
        Pod pod = new Pod("testPod");
        PodLifecycleManager.create(pod);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(pod.getPodId())));
        PodLifecycleManager.create(pod);
    }

    @Test(groups = "unit")
    public void testDelete() throws Exception {
        PodLifecycleManager.delete("testPod");
        Pod pod = new Pod("testPod");
        PodLifecycleManager.create(pod);
        Assert.assertTrue(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(pod.getPodId())));
        PodLifecycleManager.delete("testPod");
        Assert.assertFalse(CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(pod.getPodId())));
    }

    @Test(groups = "unit")
    public void testExists() throws Exception {
        Assert.assertFalse(PodLifecycleManager.exists("testPod"));
        Pod pod = new Pod("testPod");
        PodLifecycleManager.create(pod);
        Assert.assertTrue(PodLifecycleManager.exists("testPod"));
        PodLifecycleManager.delete("testPod");
        Assert.assertFalse(PodLifecycleManager.exists("testPod"));
    }

    @Test(groups = "unit")
    public void testGet() throws Exception {
        Pod in = new Pod("testPod");
        PodLifecycleManager.create(in);
        Pod out = PodLifecycleManager.get(in.getPodId());
        Assert.assertEquals(in, out);
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
        Set<Pod> in = new HashSet<Pod>();
        for (int i = 0; i < 10; ++i) {
            Pod p = new Pod(Integer.toString(i));
            in.add(p);
            PodLifecycleManager.create(p);
        }
        Assert.assertTrue(in.containsAll(PodLifecycleManager.getAll()));
    }
}
