package com.latticeengines.dataplatform.functionalframework;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.TestApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.testng.annotations.BeforeClass;

import com.latticeengines.dataplatform.entitymanager.impl.JobEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ModelEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ThrottleConfigurationEntityMgrImpl;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataPlatformFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
    private static final Log log = LogFactory.getLog(DataPlatformFunctionalTestNGBase.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private JobEntityMgrImpl jobEntityMgr;
    
    @Autowired
    private ModelEntityMgrImpl modelEntityMgr;
    
    @Autowired
    protected ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr;

    protected YarnCluster yarnCluster;

    protected YarnClient yarnClient;

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
    
    public void deleteAllStores() {
        if (jobEntityMgr != null) {
            jobEntityMgr.deleteStoreFile();
        }
        if (modelEntityMgr != null) {
            modelEntityMgr.deleteStoreFile();
        }
        if (throttleConfigurationEntityMgr != null) {
            throttleConfigurationEntityMgr.deleteStoreFile();
        }
    }
    
    public void setJobEntityMgr(JobEntityMgrImpl jobEntityMgr) {
        this.jobEntityMgr = jobEntityMgr;
    }
    
    public void setModelEntityMgr(ModelEntityMgrImpl modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
    }
    
    public void setThrottleConfigurationEntityMgr(ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
    }
    
    public String getFileUrlFromResource(String resource) {
        URL url = ClassLoader.getSystemResource(resource);
        return "file:" + url.getFile();
    }


    @BeforeClass(groups = "functional")
    public void setupRunEnvironment() throws Exception {
        log.info("Test name = " + this.getClass());
        
        deleteAllStores();
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

        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../src/main/python/launcher.py",
                "/app/dataplatform/scripts", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/lr_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/dt_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../src/main/python/algorithm/rf_train.py",
                "/app/dataplatform/scripts/algorithm", false));
        String dataplatformProps = "file:" + dataplatformPropDir + "/dataplatform.properties";
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/*.jar", "/app/dataplatform", false));
        copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/leframework.tar.gz", "/app/dataplatform/scripts", false));
        copyEntries.add(new CopyEntry(dataplatformProps, "/app/dataplatform", false));

        if (doDependencyLibraryCopy()) {
            fs.delete(new Path("/lib"), true);
            fs.mkdirs(new Path("/lib"));
            copyEntries.add(new CopyEntry("file:" + dataplatformPropDir + "/../../../target/dependency/*.jar", "/lib",
                    false));
        }

        doCopy(fs, copyEntries);
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
     * Submits application and wait state. On default waits 60 seconds.
     * 
     * @return Application info for submit
     * @throws Exception
     *             if exception occurred
     * @see ApplicationInfo
     * @see #submitApplicationAndWaitState(long, TimeUnit,
     *      YarnApplicationState...)
     */
    protected ApplicationInfo submitApplicationAndWait() throws Exception {
        return submitApplicationAndWait(60, TimeUnit.SECONDS);
    }

    /**
     * Submits application and wait state.
     * 
     * @param timeout
     *            the timeout for wait
     * @param unit
     *            the unit for timeout
     * @return Application info for submit
     * @throws Exception
     *             if exception occurred
     * @see ApplicationInfo
     * @see #submitApplicationAndWaitState(long, TimeUnit,
     *      YarnApplicationState...)
     */
    protected ApplicationInfo submitApplicationAndWait(long timeout, TimeUnit unit) throws Exception {
        return submitApplicationAndWaitState(timeout, unit, YarnApplicationState.FINISHED, YarnApplicationState.FAILED);
    }

    /**
     * Submits application and wait state. Returned state is <code>NULL</code>
     * if something failed or final known state after the wait/poll operations.
     * Array of application states can be used to return immediately from wait
     * loop if state is matched.
     * 
     * @param timeout
     *            the timeout for wait
     * @param unit
     *            the unit for timeout
     * @param applicationStates
     *            the application states to wait
     * @return Application info for submit
     * @throws Exception
     *             if exception occurred
     * @see ApplicationInfo
     */
    protected ApplicationInfo submitApplicationAndWaitState(long timeout, TimeUnit unit,
            YarnApplicationState... applicationStates) throws Exception {
        Assert.notEmpty(applicationStates, "Need to have at least one state");
        Assert.notNull(yarnClient, "Yarn client must be set");

        YarnApplicationState state = null;
        ApplicationId applicationId = submitApplication();
        Assert.notNull(applicationId, "Failed to get application id from submit");

        long end = System.currentTimeMillis() + unit.toMillis(timeout);

        // break label for inner loop
        done: do {
            state = findState(yarnClient, applicationId);
            if (state == null) {
                break;
            }
            for (YarnApplicationState stateCheck : applicationStates) {
                if (state.equals(stateCheck)) {
                    break done;
                }
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() < end);
        return new ApplicationInfo(state, applicationId);
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
     * Waits state. Returned state is <code>NULL</code> if something failed or
     * final known state after the wait/poll operations. Array of application
     * states can be used to return immediately from wait loop if state is
     * matched.
     * 
     * @param applicationId
     *            the application id
     * @param timeout
     *            the timeout for wait
     * @param unit
     *            the unit for timeout
     * @param applicationStates
     *            the application states to wait
     * @return Last known application state or <code>NULL</code> if timeout
     * @throws Exception
     *             if exception occurred
     */
    public YarnApplicationState waitState(ApplicationId applicationId, long timeout, TimeUnit unit,
            YarnApplicationState... applicationStates) throws Exception {
        Assert.notNull(yarnClient, "Yarn client must be set");
        Assert.notNull(applicationId, "ApplicationId must not be null");

        YarnApplicationState state = null;
        long end = System.currentTimeMillis() + unit.toMillis(timeout);

        // break label for inner loop
        done: do {
            state = findState(yarnClient, applicationId);
            if (state == null) {
                break;
            }
            for (YarnApplicationState stateCheck : applicationStates) {
                if (state.equals(stateCheck)) {
                    break done;
                }
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() < end);
        return state;
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

    /**
     * Get the current application state.
     * 
     * @param applicationId
     *            Yarn app application id
     * @return Current application state or <code>NULL</code> if not found
     */
    protected YarnApplicationState getState(ApplicationId applicationId) {
        Assert.notNull(yarnClient, "Yarn client must be set");
        Assert.notNull(applicationId, "ApplicationId must not be null");
        return findState(yarnClient, applicationId);
    }

    /**
     * Finds the current application state.
     * 
     * @param client
     *            the Yarn client
     * @param applicationId
     *            Yarn app application id
     * @return Current application state or <code>NULL</code> if not found
     */
    private YarnApplicationState findState(YarnClient client, ApplicationId applicationId) {
        YarnApplicationState state = null;
        for (ApplicationReport report : client.listApplications()) {
            if (report.getApplicationId().toString().equals(applicationId.toString())) {
                state = report.getYarnApplicationState();
                break;
            }
        }
        return state;
    }
}
