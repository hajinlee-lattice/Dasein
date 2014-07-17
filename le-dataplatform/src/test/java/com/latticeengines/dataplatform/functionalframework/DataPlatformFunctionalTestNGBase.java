package com.latticeengines.dataplatform.functionalframework;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.test.context.YarnCluster;
import org.springframework.yarn.test.junit.ApplicationInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.dataplatform.entitymanager.AlgorithmEntityMgr;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
@Transactional
public class DataPlatformFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(DataPlatformFunctionalTestNGBase.class);
    public static final EnumSet<FinalApplicationStatus> TERMINAL_STATUS = EnumSet.of(FinalApplicationStatus.FAILED,
            FinalApplicationStatus.KILLED, FinalApplicationStatus.SUCCEEDED);
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 20;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected JobEntityMgr jobEntityMgr;

    @Autowired
    protected ModelEntityMgr modelEntityMgr;

    @Autowired
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @Autowired
    protected AlgorithmEntityMgr algorithmEntityMgr;

    @Autowired
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Value("${dataplatform.dlorchestration.datasource.host}")
    protected String dbDlOrchestrationHost;

    @Value("${dataplatform.dlorchestration.datasource.port}")
    protected int dbDlOrchestrationPort;

    @Value("${dataplatform.dlorchestration.datasource.dbname}")
    protected String dbDlOrchestrationName;

    @Value("${dataplatform.dlorchestration.datasource.user}")
    protected String dbDlOrchestrationUser;

    @Value("${dataplatform.dlorchestration.datasource.password.encrypted}")
    protected String dbDlOrchestrationPassword;

    @Value("${dataplatform.dlorchestration.datasource.type}")
    protected String dbDlOrchestrationType;

    @Value("${dataplatform.container.virtualcores}")
    protected int virtualCores;

    @Value("${dataplatform.container.memory}")
    protected int memory;

    protected YarnCluster yarnCluster;

    protected YarnClient yarnClient;

    protected ScheduledExecutorService executor;

    public DataPlatformFunctionalTestNGBase() {
    }

    public DataPlatformFunctionalTestNGBase(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    protected boolean doYarnClusterSetup() {
        return true;
    }

    protected boolean doDependencyLibraryCopy() {
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

    public void setJobEntityMgr(JobEntityMgr jobEntityMgr) {
        this.jobEntityMgr = jobEntityMgr;
    }

    public void setModelEntityMgr(ModelEntityMgr modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
    }

    public void setThrottleConfigurationEntityMgr(ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
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

    @BeforeClass(groups = { "functional", "functional.scheduler" })
    public void setupRunEnvironment() throws Exception {
        log.info("Test name = " + this.getClass());

        if (!doYarnClusterSetup()) {
            return;
        }

        FileSystem fs = FileSystem.get(yarnConfiguration);
        // Delete directories
        fs.delete(new Path("/app"), true);

        // Make directories
        fs.mkdirs(new Path("/app/dataplatform/scripts"));
        fs.mkdirs(new Path("/app/dataplatform/scripts/leframework"));

        // Copy jars from build to hdfs
        String dataplatformPropDir = System.getProperty("DATAPLATFORM_PROPDIR");
        if (StringUtils.isEmpty(dataplatformPropDir)) {
            dataplatformPropDir = System.getenv().get("DATAPLATFORM_PROPDIR");
        }
        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/hadoop-metrics2.properties",
                "/app/dataplatform", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../src/main/python/launcher.py",
                "/app/dataplatform/scripts", false));
        copyEntries.add(new CopyEntry(
                "file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/lr_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        copyEntries.add(new CopyEntry(
                "file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/dt_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        copyEntries.add(new CopyEntry(
                "file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/rf_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir
                + "/../../../src/main/python/algorithm/data_profile.py", "/app/dataplatform/scripts/algorithm", false));
        String dataplatformProps = "file:" + dataplatformPropDir + "/dataplatform.properties";
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/*.jar", "/app/dataplatform",
                false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/leframework.tar.gz",
                "/app/dataplatform/scripts", false));
        copyEntries.add(new CopyEntry(dataplatformProps, "/app/dataplatform", false));

        if (doDependencyLibraryCopy()) {
            fs.delete(new Path("/lib"), true);
            fs.mkdirs(new Path("/lib"));
            copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/dependency/*.jar", "/lib",
                    false));
        }

        doCopy(fs, copyEntries);
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
}
