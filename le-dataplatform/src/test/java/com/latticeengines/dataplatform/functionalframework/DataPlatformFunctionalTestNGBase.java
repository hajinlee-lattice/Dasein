package com.latticeengines.dataplatform.functionalframework;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.Assert;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.junit.ApplicationInfo;
import org.testng.annotations.AfterClass;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataPlatformFunctionalTestNGBase extends YarnFunctionalTestNGBase
        implements DataplatformFunctionalTestNGInterface {

    private static final Logger log = LoggerFactory.getLogger(DataPlatformFunctionalTestNGBase.class);

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private OrderedEntityMgrListForDbClean orderedEntityMgrListForDbClean;

    @Deprecated
    @Value("${dataplatform.test.datasource.host}")
    protected String dataSourceHost;

    @Deprecated
    @Value("${dataplatform.test.datasource.port}")
    protected int dataSourcePort;

    @Deprecated
    @Value("${dataplatform.test.datasource.dbname}")
    protected String dataSourceDB;

    @Deprecated
    @Value("${dataplatform.test.datasource.user}")
    protected String dataSourceUser;

    @Deprecated
    @Value("${dataplatform.test.datasource.password.encrypted}")
    protected String dataSourcePasswd;

    @Deprecated
    @Value("${dataplatform.test.datasource.type}")
    protected String dataSourceDBType;

    @Value("${dataplatform.container.virtualcores}")
    protected int virtualCores;

    @Value("${dataplatform.container.memory}")
    protected int memory;

    private ScheduledExecutorService executor;

    public DataPlatformFunctionalTestNGBase() {
    }

    public DataPlatformFunctionalTestNGBase(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    protected boolean doClearDbTables() {
        return true;
    }

    protected void cleanUpHdfs(String customer) {
        String deletePath = customerBaseDir + "/" + customer + "/data";
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, deletePath)) {
                HdfsUtils.rmdir(yarnConfiguration, deletePath);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16001, e, new String[] { deletePath });
        }
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
     * this helper method produces 1 definition with 3 algorithms
     *
     * @return
     */
    protected ModelDefinition produceModelDefinition() {
        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(2);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model-" + System.currentTimeMillis());
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { //
                decisionTreeAlgorithm, //
                randomForestAlgorithm, //
                logisticRegressionAlgorithm }));

        return modelDef;
    }

    /**
     * this helper method produces a Model for unit / functional test (note:
     * ModelDefinition still needs to be set)
     *
     * @param appIdStr
     * @return
     */
    protected Model produceIrisMetadataModel() {
        String suffix = Long.toString(System.currentTimeMillis());
        Model model = new Model();
        model.setName("Model Submission for Demo " + suffix);
        model.setId(UUID.randomUUID().toString());
        model.setTable("iris");
        model.setMetadataTable("iris_metadata");
        model.setFeaturesList(Arrays.<String> asList(new String[] { "SEPAL_LENGTH", //
                "SEPAL_WIDTH", //
                "PETAL_LENGTH", //
                "PETAL_WIDTH" }));
        model.setTargetsList(Arrays.<String> asList(new String[] { "CATEGORY" }));
        model.setCustomer("INTERNAL");
        model.setKeyCols(Arrays.<String> asList(new String[] { "ID" }));
        model.setDataFormat("avro");

        return model;
    }

    public NumberFormat getAppIdFormat() {
        NumberFormat fmt = NumberFormat.getInstance();
        fmt.setGroupingUsed(false);
        fmt.setMinimumIntegerDigits(4);
        return fmt;
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
     * Gets the {@link YarnClient}.
     *
     * @return the Yarn client
     */
    public YarnClient getYarnClient() {
        return yarnClient;
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
     * Submits application and wait status. Returned status is <code>NULL</code>
     * if something failed or final known status after the wait/poll operations.
     * Array of application statuses can be used to return immediately from wait
     * loop if status is matched.
     *
     * @param applicationStatuses
     *            the application statuses to wait
     * @return Application id for submit
     * @throws Exception
     *             if exception occurred
     * @see ApplicationInfo
     */
    protected ApplicationId submitApplicationAndWaitStatus(FinalApplicationStatus... applicationStatuses)
            throws Exception {
        Assert.notEmpty(applicationStatuses, "Need to have at least one status");

        ApplicationId applicationId = submitApplication();

        waitForStatus(applicationId, applicationStatuses);

        return applicationId;
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

    /**
     * Kill the application.
     *
     * @param applicationId
     *            the application id
     */
    protected void killApplication(ApplicationId applicationId) {
        Assert.notNull(yarnClient, "Yarn client must be set");
        Assert.notNull(applicationId, "ApplicationId must not be null");
        yarnClient.killApplication(applicationId);
    }

    protected FinalApplicationStatus getStatus(ApplicationId applicationId) {
        Assert.notNull(yarnClient, "Yarn client must be set");
        Assert.notNull(applicationId, "ApplicationId must not be null");
        return findStatus(yarnClient, applicationId.toString());
    }

    protected void startQuartzJob(Runnable command, long period) {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(command, 0L, period, TimeUnit.SECONDS);
    }

    protected void stopQuartzJob() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(500L, TimeUnit.MILLISECONDS);
            if (!executor.isTerminated()) {
                log.warn("Quartz thread is not shut down properly");
            }
        } catch (InterruptedException e) {
            log.error("Can't shut down quartz thread due to: " + ExceptionUtils.getStackTrace(e));
        }
    }

}
