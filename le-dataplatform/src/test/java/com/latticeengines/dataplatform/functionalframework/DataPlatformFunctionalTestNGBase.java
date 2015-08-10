package com.latticeengines.dataplatform.functionalframework;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.latticeengines.common.exposed.util.HdfsUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.TestApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.test.context.YarnCluster;
import org.springframework.yarn.test.junit.ApplicationInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataPlatformFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(DataPlatformFunctionalTestNGBase.class);
    public static final EnumSet<FinalApplicationStatus> TERMINAL_STATUS = EnumSet.of(FinalApplicationStatus.FAILED,
            FinalApplicationStatus.KILLED, FinalApplicationStatus.SUCCEEDED);
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 20;

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private OrderedEntityMgrListForDbClean orderedEntityMgrListForDbClean;

    @Value("${dataplatform.test.datasource.host}")
    protected String dataSourceHost;

    @Value("${dataplatform.test.datasource.port}")
    protected int dataSourcePort;

    @Value("${dataplatform.test.datasource.dbname}")
    protected String dataSourceDB;

    @Value("${dataplatform.test.datasource.user}")
    protected String dataSourceUser;

    @Value("${dataplatform.test.datasource.password.encrypted}")
    protected String dataSourcePasswd;

    @Value("${dataplatform.test.datasource.type}")
    protected String dataSourceDBType;

    @Value("${dataplatform.container.virtualcores}")
    protected int virtualCores;

    @Value("${dataplatform.container.memory}")
    protected int memory;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    protected YarnCluster yarnCluster;

    protected YarnClient yarnClient;

    protected ScheduledExecutorService executor;

    public DataPlatformFunctionalTestNGBase() {
    }

    public DataPlatformFunctionalTestNGBase(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    protected boolean doClearDbTables() {
        return true;
    }

    public File[] getAvroFilesForDir(String parentDir) {
        return new File(parentDir).listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".avro");
            }

        });
    }

    public String getFileUrlFromResource(String resource) {
        URL url = ClassLoader.getSystemResource(resource);
        return "file:" + url.getFile();
    }

    @BeforeMethod(enabled = true, firstTimeOnly = true, alwaysRun = true)
    public void beforeEachTest() {
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
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
            log.warn("Could not clear tables for all entity managers.", e);
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
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                randomForestAlgorithm, logisticRegressionAlgorithm }));

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

    public ApplicationId getApplicationId(String appIdStr) {
        String[] tokens = appIdStr.split("_");
        TestApplicationId appId = new TestApplicationId();
        appId.setClusterTimestamp(Long.parseLong(tokens[1]));
        appId.setId(Integer.parseInt(tokens[2]));
        appId.build();
        return appId;
    }

    public NumberFormat getAppIdFormat() {
        NumberFormat fmt = NumberFormat.getInstance();
        fmt.setGroupingUsed(false);
        fmt.setMinimumIntegerDigits(4);
        return fmt;
    }

    public void doCopy(FileSystem fs, List<CopyEntry> copyEntries) throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        for (CopyEntry e : copyEntries) {
            for (String pattern : StringUtils.commaDelimitedListToStringArray(e.getSrc())) {
                for (Resource res : resolver.getResources(pattern)) {
                    Path destinationPath = getDestinationPath(e, res);
                    FSDataOutputStream os = fs.create(destinationPath);
                    FileCopyUtils.copy(res.getInputStream(), os);
                }
            }
        }

    }

    protected Path getDestinationPath(CopyEntry entry, Resource res) throws IOException {
        Path dest = new Path(entry.getDest(), res.getFilename());
        return dest;
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
     * Gets the running {@link YarnCluster} for tests.
     *
     * @return the Yarn cluster
     */
    public YarnCluster getYarnCluster() {
        return yarnCluster;
    }

    /**
     * Sets the {@link YarnCluster}
     *
     * @param yarnCluster
     *            the Yarn cluster
     */
    public void setYarnCluster(YarnCluster yarnCluster) {
        this.yarnCluster = yarnCluster;
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
    @Autowired
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

    public FinalApplicationStatus waitForStatus(ApplicationId applicationId,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        Assert.notNull(yarnClient, "Yarn client must be set");
        Assert.notNull(applicationId, "ApplicationId must not be null");

        FinalApplicationStatus status = null;
        long start = System.currentTimeMillis();

        // break label for inner loop
        done: do {
            status = findStatus(yarnClient, applicationId);
            if (status == null) {
                break;
            }
            for (FinalApplicationStatus statusCheck : applicationStatuses) {
                if (status.equals(statusCheck) || TERMINAL_STATUS.contains(status)) {
                    break done;
                }
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() - start < MAX_MILLIS_TO_WAIT);
        return status;
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
        return findStatus(yarnClient, applicationId);
    }

    private FinalApplicationStatus findStatus(YarnClient client, ApplicationId applicationId) {
        FinalApplicationStatus status = null;
        for (ApplicationReport report : client.listApplications()) {
            if (report.getApplicationId().toString().equals(applicationId.toString())) {
                status = report.getFinalApplicationStatus();
                break;
            }
        }
        return status;
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

    protected String generateUnique() {
        return generateUnique("");
    }

    protected String generateUnique(String base) {
        String id = UUID.randomUUID().toString();
        return base.equals("") ? id : (base + "_" + id);
    }

}
