package com.latticeengines.dataplatform.functionalframework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.ClientRmTemplate;
import org.springframework.yarn.client.CommandYarnClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ProxyUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.runtime.mapreduce.MRJobCustomizationBase;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.DefaultYarnClientCustomization;
import com.latticeengines.dataplatform.exposed.yarn.client.YarnClientCustomization;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.dataplatform.service.impl.YarnClientCustomizationServiceImpl;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class DataplatformMiniClusterFunctionalTestNG extends DataPlatformFunctionalTestNGBase {

    protected static final Log log = LogFactory.getLog(DataplatformMiniClusterFunctionalTestNG.class);

    @Autowired
    protected JobService jobService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected Configuration hadoopConfiguration;

    @Autowired
    protected VersionManager versionManager;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

    protected Configuration miniclusterConfiguration;

    private MiniYARNCluster miniCluster;

    private MiniDFSCluster hdfsCluster;

    @Value("${dataplatform.hdfs.stack:}")
    protected String stackName;

    @Value("${dataplatform.queue.scheme:legacy}")
    protected String queueScheme;

    public static final String JACOCO_AGENT_FILE = System.getenv("JACOCO_AGENT_FILE");

    public static final String JACOCO_DEST_FILE = System.getenv("JACOCO_DEST_FILE");

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupMiniCluster();
        uploadArtifactsToHdfs();
    }

    @AfterClass(groups = "functional")
    public void clear() throws IOException {
        miniCluster.close();
        hdfsCluster.shutdown();
    }

    protected void setupMiniCluster() throws IOException {
        miniclusterConfiguration = new YarnConfiguration();
        miniclusterConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2024);
        miniclusterConfiguration.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 300);
        miniclusterConfiguration.set(YarnConfiguration.NM_LOG_DIRS, ".");

        miniclusterConfiguration.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
                ResourceScheduler.class);
        miniCluster = new MiniYARNCluster("minicluster", 1, 1, 1);

        String jacocoOpt = String.format(" -javaagent:%s=destfile=%s,append=true,includes=com.*", JACOCO_AGENT_FILE,
                JACOCO_DEST_FILE);
        miniclusterConfiguration.set(MRJobConfig.MAP_JAVA_OPTS, jacocoOpt);
        miniclusterConfiguration.set(MRJobConfig.REDUCE_JAVA_OPTS, jacocoOpt);
        miniCluster.init(miniclusterConfiguration);
        miniCluster.start();

        miniclusterConfiguration = new YarnConfiguration(miniCluster.getConfig());
        miniclusterConfiguration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, ".");
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(miniclusterConfiguration);
        hdfsCluster = builder.build();
    }

    protected void uploadArtifactsToHdfs() throws IOException {
        String dpHdfsPath = String.format("/app/%s/dataplatform", versionManager.getCurrentVersionInStack(stackName))
                .toString();
        FileUtils.deleteDirectory(new File("dataplatform"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, dpHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "dataplatform", dpHdfsPath);

        String lepropertiesPath = String.format("/app/%s/conf/latticeengines.properties",
                versionManager.getCurrentVersionInStack(stackName));
        Properties properties = regenerateProperties(lepropertiesPath);

        FileUtils.deleteQuietly(new File("latticeengines.properties"));
        properties.store(new FileOutputStream("latticeengines.properties"), "");
        FileUtils.deleteQuietly(new File(".latticeengines.properties.crc"));
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "latticeengines.properties", lepropertiesPath);
    }

    protected Properties regenerateProperties(String lepropertiesPath) throws IOException {
        Properties properties = new Properties();
        properties.load(HdfsUtils.getInputStream(yarnConfiguration, lepropertiesPath));
        properties.setProperty("hadoop." + YarnConfiguration.RM_ADDRESS,
                miniclusterConfiguration.get(YarnConfiguration.RM_ADDRESS));
        properties.setProperty("hadoop." + YarnConfiguration.RM_WEBAPP_ADDRESS,
                miniclusterConfiguration.get(YarnConfiguration.RM_WEBAPP_ADDRESS));
        properties.setProperty("hadoop." + YarnConfiguration.RM_SCHEDULER_ADDRESS,
                miniclusterConfiguration.get(YarnConfiguration.RM_SCHEDULER_ADDRESS));
        properties.setProperty("hadoop." + FileSystem.FS_DEFAULT_NAME_KEY,
                "hdfs://localhost:" + hdfsCluster.getNameNodePort());
        properties.setProperty("swlib." + FileSystem.FS_DEFAULT_NAME_KEY,
                "hdfs://localhost:" + hdfsCluster.getNameNodePort());
        return properties;
    }

    public <T extends MRJobCustomizationBase> JobID testMRJob(Class<T> mrJobCustomizationClass, Properties properties)
            throws Exception {
        Constructor<T> constructor = mrJobCustomizationClass.getConstructor(Configuration.class);
        MRJobCustomizationBase mrJobCustomization = constructor.newInstance(miniclusterConfiguration);

        JobConf jobConf = new JobConf(mrJobCustomization.getConf(), mrJobCustomization.getClass());
        @SuppressWarnings("deprecation")
        Job mrJob = new Job(jobConf);

        mrJobCustomization.customize(mrJob, properties);
        return JobService.runMRJob(mrJob, "jobName", true);
    }

    public ApplicationId testYarnJob(String yarnClientName, Properties appMasterProperties,
            Properties containerProperties) throws Exception {
        ((YarnClientCustomizationServiceImpl) ProxyUtils.getTargetObject(yarnClientCustomizationService))
                .setConfiguration(miniclusterConfiguration);
        YarnClientCustomization customization = YarnClientCustomization.getCustomization(yarnClientName);
        ((DefaultYarnClientCustomization) ProxyUtils.getTargetObject(customization))
                .setConfiguration(miniclusterConfiguration);

        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.overwriteQueueAssignment(
                appMasterProperties.getProperty(AppMasterProperty.QUEUE.name()), queueScheme));

        containerProperties.put(ContainerProperty.JACOCO_AGENT_FILE.name(), JACOCO_AGENT_FILE);
        containerProperties.put(ContainerProperty.JACOCO_DEST_FILE.name(), JACOCO_DEST_FILE);

        ClientRmTemplate clientTemplate = new ClientRmTemplate(miniclusterConfiguration);
        clientTemplate.afterPropertiesSet();
        yarnClient = new CommandYarnClient(clientTemplate);
        ((CommandYarnClient) yarnClient).setConfiguration(miniclusterConfiguration);

        ((CommandYarnClient) yarnClient)
                .setEnvironment(((CommandYarnClient) (applicationContext.getBean(yarnClientName))).getEnvironment());
        return jobService.submitYarnJob(((CommandYarnClient) yarnClient), yarnClientName, appMasterProperties,
                containerProperties);
    }
}
