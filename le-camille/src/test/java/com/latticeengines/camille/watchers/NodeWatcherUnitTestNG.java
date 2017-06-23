package com.latticeengines.camille.watchers;

import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.domain.exposed.camille.Document;

public class NodeWatcherUnitTestNG {

    @BeforeClass(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testWatch() throws Exception {
        String watcher = "TestWatcher";
        NodeWatcher.registerWatcher(watcher);
        TestListener listener1 = new TestListener();
        TestListener listener2 = new TestListener();
        Assert.assertEquals(listener1.state, 0);
        Assert.assertEquals(listener2.state, 0);

        NodeWatcher.registerListener(watcher, listener1);
        Camille camille = CamilleEnvironment.getCamille();
        camille.upsert(NodeWatcher.getWatcherPath(watcher), new Document(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Thread.sleep(1000L);
        Assert.assertEquals(listener1.state, 1);
        Assert.assertEquals(listener2.state, 0);

        NodeWatcher.registerListener(watcher, listener2);
        camille.upsert(NodeWatcher.getWatcherPath(watcher), new Document(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Thread.sleep(1000L);
        Assert.assertEquals(listener1.state, 2);
        Assert.assertEquals(listener2.state, 1);
    }

    private static class TestListener implements NodeCacheListener {
        private int state = 0;

        @Override
        public void nodeChanged() throws Exception {
            state++;
        }

        public int getState() {
            return state;
        }
    }

}
