package com.latticeengines.dataplatform.functionalframework;

import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.Assert;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.AfterClass;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataPlatformFunctionalTestNGBase extends YarnFunctionalTestNGBase
        implements DataplatformFunctionalTestNGInterface {

    private static final Logger log = LoggerFactory.getLogger(DataPlatformFunctionalTestNGBase.class);

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private OrderedEntityMgrListForDbClean orderedEntityMgrListForDbClean;

    @Value("${dataplatform.container.virtualcores}")
    protected int virtualCores;

    @Value("${dataplatform.container.memory}")
    protected int memory;

    private ScheduledExecutorService executor;

    public DataPlatformFunctionalTestNGBase() {
    }

    protected boolean doClearDbTables() {
        return true;
    }

    @AfterClass(groups = { "functional", "functional.scheduler" })
    public void clearTables() {
        if (!doClearDbTables()) {
            return;
        }
        try {
            for (BaseEntityMgr<?> entityMgr : orderedEntityMgrListForDbClean.entityMgrs()) {
                entityMgr.deleteAll();
            }
        } catch (Exception e) {
            log.warn("Could not clear tables for all entity managers.");
        }
    }

    /**
     * Gets the running cluster runtime {@link Configuration} for tests.
     *
     * @return the Yarn cluster config
     */
    public Configuration getConfiguration() {
        return yarnConfiguration;
    }

    /**
     * Sets the {@link YarnClient}.
     *
     * @param yarnClient
     *            the Yarn client
     */
    @Override
    public void setYarnClient(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    /**
     * Submit an application.
     *
     * @return the submitted application {@link ApplicationId}
     */
    protected ApplicationId submitApplication() {
        Assert.notNull(yarnClient, "Yarn client must be set");
        ApplicationId applicationId = yarnClient.submitApplication();
        Assert.notNull(applicationId, "Failed to get application id from submit");
        return applicationId;
    }

}
