package com.latticeengines.apps.cdl.entitymgr.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author jadusumalli Test method to simulate Load on DB Connection pool Adjust
 *         the Total Threads and Pool Size to validate for different load
 *         scenario
 */
public class RatingEngineLoadTestingTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineLoadTestingTestNG.class);

    private static final int TEST_THREADS = 30;
    private static final int TEST_THREAD_POOL_SIZE = 4;

    private static final String RATING_ENGINE_NOTE = "LoadTesting - Rating Engine Test";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private List<DataSource> datasources;

    private DataSourceStatusThread dsStatusThread;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        log.info("************* Setup Test ************ Total Threads:{}, PoolSize:{}", TEST_THREADS,
                TEST_THREAD_POOL_SIZE);
        setupTestEnvironmentWithDummySegment();
        createRatingEngine();
        ActionContext.remove();

        dsStatusThread = new DataSourceStatusThread(datasources);
        dsStatusThread.setDaemon(true);
        dsStatusThread.start();
        log.info("************* Setup Completed ************");
    }

    protected RatingEngine createRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setId(UUID.randomUUID().toString());

        return ratingEngine;
    }

    @AfterClass(groups = "functional")
    public void cleanupActionContext() {
        // Stop the DSStatus thread.
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        dsStatusThread.interrupted = true;
        log.info("Marked interrupted to true ");
        ActionContext.remove();
    }

    private RatingEngine testCreation() {
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(createRatingEngine());
        Assert.assertNotNull(createdRatingEngine);
        log.info("Rating Engine created {} - {}", createdRatingEngine.getId(), createdRatingEngine.getDisplayName());
        return createdRatingEngine;
    }

    private void testFind() {
        List<RatingEngine> ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        log.info("Rating Engine Listing: {} ", ratingEngineList.size());
        ratingEngineEntityMgr.findAllDeleted();
    }

    private void testFindById(String id) {
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id);
        Assert.assertNotNull(ratingEngine);
        Assert.assertEquals(ratingEngine.getId(), id);
    }

    @Test(groups = "functional")
    public void testBasicOps() {
        log.info("************* Main Test ************");
        testCreation();
        testFind();
        log.info("------------ End of Main Test ------------");
    }

    @Test(groups = "functional")
    public void testWithMultipleThreads() {
        ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool("re-load-test", TEST_THREAD_POOL_SIZE);
        List<Runnable> runnables = new ArrayList<>();
        for (int jobIndex = 0; jobIndex < TEST_THREADS; jobIndex++) {
            runnables.add(new RunnableTest(this, MultiTenantContext.getTenant(), jobIndex + 1));
        }
        log.info("Going to submited {} threads: {} ", TEST_THREADS);
        ThreadPoolUtils.runRunnablesInParallel(executor, runnables, 10, 1);
    }

    public class RunnableTest implements Runnable {
        final RatingEngineLoadTestingTestNG ret;
        final Tenant tenant;
        final int count;

        RunnableTest(RatingEngineLoadTestingTestNG ret, Tenant tenant, int index) {
            this.ret = ret;
            this.tenant = tenant;
            this.count = index;
        }

        @Override
        public void run() {
            try {
                log.info("**** In Side Thread - {} - Index: {}", Thread.currentThread().getName(), count);
                MultiTenantContext.setTenant(tenant);
                RatingEngine re = ret.testCreation();
                Assert.assertNotNull(re);
                ret.testFindById(re.getId());
                log.info("---- End of Thread - {} - Index: {}", Thread.currentThread().getName(), count);
            } catch (Exception e) {
                log.error("#### Error while performing DB op: - Index: " + count, e);
            }
        }
    }

    public class DataSourceStatusThread extends Thread {
        final List<? extends DataSource> dsList;
        volatile boolean interrupted = false;

        DataSourceStatusThread(List<DataSource> datasources) {
            this.dsList = datasources;
        }

        @Override
        public void run() {
            if (dsList == null) {
                log.error("Could not find any DataSources for monitoing");
                return;
            }

            // for (DataSource cpds : dsList) {
            // log.info("Connection Pool Initial Config: {}", (cpds
            // instanceof ComboPooledDataSource) ?
            // ((ComboPooledDataSource)cpds).toString(true) :
            // cpds.toString());
            // }

            try {
                while (!interrupted) {
                    StringBuilder sb = new StringBuilder();

                    sb.append(String.format("\n %35s %10s %10s %10s %10s %10s %10s %10s %10s %10s", "DataSource Name",
                            "Max Conns", "Allocated", "In Use", "Idle", "Orphaned", "Failed CO", "Failed CI",
                            "Helper Ts", "Pending Tasks"));
                    for (DataSource ds : dsList) {
                        if (!(ds instanceof ComboPooledDataSource)) {
                            continue;
                        }
                        ComboPooledDataSource cpds = (ComboPooledDataSource) ds;
                        if (cpds.getDataSourceName().contains("hive")) {
                            continue;
                        }
                        try {
                            sb.append(String.format("\n %35s %10s %10s %10s %10s %10s %10s %10s %10s %10s",
                                    cpds.getDataSourceName(), cpds.getMaxPoolSize(), cpds.getNumConnections(),
                                    cpds.getNumBusyConnections(), cpds.getNumIdleConnections(),
                                    cpds.getNumUnclosedOrphanedConnections(), cpds.getNumFailedCheckoutsDefaultUser(),
                                    cpds.getNumFailedCheckinsDefaultUser(), cpds.getNumHelperThreads(),
                                    cpds.getThreadPoolNumTasksPending()));
                        } catch (SQLException e) {
                            sb.append("Exception:").append(e.getMessage());
                        }

                    }
                    log.info("Connection Pool Status: {}", sb.toString());
                    long sleepTime = 500L;
                    log.info("*** Sleeping for {} secs. {}", sleepTime, interrupted);
                    Thread.sleep(sleepTime);
                    log.info("*** After Sleeping . {}", interrupted);
                }
            } catch (InterruptedException e) {
                log.error("Got interrupted: " + e.getMessage());
            }

        }

    }

}
