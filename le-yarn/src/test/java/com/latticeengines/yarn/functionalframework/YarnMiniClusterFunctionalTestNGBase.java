package com.latticeengines.yarn.functionalframework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.LedpMRAppMaster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.ClientRmTemplate;
import org.springframework.yarn.client.CommandYarnClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.client.DefaultYarnClientCustomization;
import com.latticeengines.yarn.exposed.client.YarnClientCustomization;
import com.latticeengines.yarn.exposed.runtime.mapreduce.MRJobCustomizationBase;
import com.latticeengines.yarn.exposed.service.EMREnvService;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.YarnClientCustomizationService;
import com.latticeengines.yarn.exposed.service.impl.YarnClientCustomizationServiceImpl;

public class YarnMiniClusterFunctionalTestNGBase extends YarnFunctionalTestNGBase {

    protected static final Logger log = LoggerFactory.getLogger(YarnMiniClusterFunctionalTestNGBase.class);

    @Autowired
    protected JobService jobService;

    @Inject
    protected ManifestService manifestService;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

    protected Configuration miniclusterConfiguration;

    private MiniYARNCluster miniCluster;

    private MiniDFSCluster hdfsCluster;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${yarn.use.minicluster}")
    private boolean useMiniCluster;

    private static final String JACOCO_AGENT_FILE = System.getenv("JACOCO_AGENT_FILE");

    private String jacocoDestFile;
    private String miniClusterName;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        log.info("useMiniCluster=" + useMiniCluster);
        if (useMiniCluster) {
            setupMiniCluster();
            uploadArtifactsToHdfs();
        } else {
            miniclusterConfiguration = yarnConfiguration;
        }
    }

    @AfterClass(groups = "functional")
    public void clear() throws IOException {
        if (useMiniCluster) {
            miniCluster.close();
            hdfsCluster.shutdown();
        }
    }

    private void setupMiniCluster() throws IOException {
        miniclusterConfiguration = new YarnConfiguration();
        miniclusterConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2024);
        miniclusterConfiguration.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 300);
        String logDir = new File(".").getAbsolutePath();
        log.info("Using log dir " + logDir);
        miniclusterConfiguration.set(YarnConfiguration.NM_LOG_DIRS, logDir);
        int port = findAvaliablePort();
        log.info("Found an available local port " + port + " for mapreduce.shuffle.port");
        miniclusterConfiguration.setInt("mapreduce.shuffle.port", port);

        miniclusterConfiguration.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
                ResourceScheduler.class);
        miniClusterName = NamingUtils.uuid("minicluster");
        log.info("MiniCluster Name is " + miniClusterName);
        miniCluster = new MiniYARNCluster(miniClusterName, 1, 1, 1);

        if (StringUtils.isNotBlank(JACOCO_AGENT_FILE)) {
            jacocoDestFile = System.getenv("JACOCO_DEST_FILE");
            if (StringUtils.isBlank(jacocoDestFile)) {
                jacocoDestFile = logDir + "/jacoco.exec";
            }
            String jacocoOpt = String.format(" -javaagent:%s=destfile=%s,append=true,includes=com.*", JACOCO_AGENT_FILE,
                    jacocoDestFile);
            miniclusterConfiguration.set(MRJobConfig.MAP_JAVA_OPTS, jacocoOpt);
            miniclusterConfiguration.set(MRJobConfig.REDUCE_JAVA_OPTS, jacocoOpt);
        }
        log.info("Initializing mini yarn cluster ...");
        miniCluster.init(miniclusterConfiguration);
        miniCluster.start();

        miniclusterConfiguration = new YarnConfiguration(miniCluster.getConfig());
        miniclusterConfiguration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, ".");
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(miniclusterConfiguration);
        log.info("Initializing mini hdfs cluster ...");
        hdfsCluster = builder.build();
    }

    protected void uploadArtifactsToHdfs() throws IOException {
        String dpHdfsPath = String.format("%s/dataplatform", manifestService.getLedpPath());
        FileUtils.deleteQuietly(new File("dataplatform"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, dpHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "dataplatform", dpHdfsPath);

        String dsHdfsPath = manifestService.getLedsPath();
        FileUtils.deleteQuietly(new File("datascience"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, dsHdfsPath, "datascience");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "datascience", dsHdfsPath);

        String log4jPath = String.format("%s/conf/log4j.properties", manifestService.getLedpPath());
        FileUtils.deleteQuietly(new File("log4j.properties"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, log4jPath, ".");
        FileUtils.deleteQuietly(new File(".log4j.properties.crc"));
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "log4j.properties", log4jPath);

        log4jPath = String.format("%s/conf/log4j2-yarn.xml", manifestService.getLedpPath());
        FileUtils.deleteQuietly(new File("log4j2-yarn.xml"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, log4jPath, ".");
        FileUtils.deleteQuietly(new File(".log4j2-yarn.xml.crc"));
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "log4j2-yarn.xml", log4jPath);

        String lepropertiesPath = String.format("%s/conf/latticeengines.properties", manifestService.getLedpPath());
        Properties properties = regenerateProperties(lepropertiesPath);

        FileUtils.deleteQuietly(new File("latticeengines.properties"));
        properties.store(new FileOutputStream("latticeengines.properties"), "");
        FileUtils.deleteQuietly(new File(".latticeengines.properties.crc"));
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "latticeengines.properties", lepropertiesPath);
    }

    private Properties regenerateProperties(String lepropertiesPath) throws IOException {
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
        // properties.setProperty("swlib." + FileSystem.FS_DEFAULT_NAME_KEY,
        // "hdfs://localhost:" + hdfsCluster.getNameNodePort());
        return properties;
    }

    public <T extends MRJobCustomizationBase> JobID testMRJob(Class<T> mrJobCustomizationClass, Properties properties)
            throws Exception {
        return testMRJob(mrJobCustomizationClass, properties, null, null);
    }

    public <T extends MRJobCustomizationBase> JobID testMRJob(Class<T> mrJobCustomizationClass, Properties properties,
            String counterGroupName, Map<String, Long> counterGroupResultMap) throws Exception {
        Job mrJob = createMRJob(mrJobCustomizationClass, properties);
        return JobService.runMRJob(mrJob, "jobName", true, counterGroupName, counterGroupResultMap);
    }

    public <T extends MRJobCustomizationBase> Job createMRJob(Class<T> mrJobCustomizationClass, Properties properties)
            throws Exception {
        Constructor<T> constructor = mrJobCustomizationClass.getConstructor(Configuration.class);
        MRJobCustomizationBase mrJobCustomization = constructor.newInstance(miniclusterConfiguration);

        JobConf jobConf = new JobConf(mrJobCustomization.getConf(), mrJobCustomization.getClass());
        @SuppressWarnings("deprecation")
        Job mrJob = new Job(jobConf);

        overwriteAMQueueAssignment(properties);
        overwriteContainerQueueAssignment(properties);
        mrJobCustomization.customize(mrJob, properties);
        if (properties != null) {
            Configuration config = mrJob.getConfiguration();
            config.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
            for (Object key : properties.keySet()) {
                config.set(key.toString(), properties.getProperty((String) key));
            }
        }
        return mrJob;
    }

    public ApplicationId testYarnJob(String yarnClientName, Properties appMasterProperties,
            Properties containerProperties) throws Exception {
        ((YarnClientCustomizationServiceImpl) yarnClientCustomizationService)
                .setConfiguration(miniclusterConfiguration);

        YarnClientCustomization customization = YarnClientCustomization.getCustomization(yarnClientName);
        ((DefaultYarnClientCustomization) customization).setConfiguration(miniclusterConfiguration);

        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.overwriteQueueAssignment(
                appMasterProperties.getProperty(AppMasterProperty.QUEUE.name()), emrEnvService.getYarnQueueScheme()));

        if (StringUtils.isNotBlank(JACOCO_AGENT_FILE) && StringUtils.isNotBlank(jacocoDestFile)) {
            containerProperties.put(ContainerProperty.JACOCO_AGENT_FILE.name(), JACOCO_AGENT_FILE);
            containerProperties.put(ContainerProperty.JACOCO_DEST_FILE.name(), jacocoDestFile);
        }

        ClientRmTemplate clientTemplate = new ClientRmTemplate(miniclusterConfiguration);
        clientTemplate.afterPropertiesSet();
        yarnClient = new CommandYarnClient(clientTemplate);
        ((CommandYarnClient) yarnClient).setConfiguration(miniclusterConfiguration);

        ((CommandYarnClient) yarnClient)
                .setEnvironment(((CommandYarnClient) (applicationContext.getBean(yarnClientName))).getEnvironment());
        return jobService.submitYarnJob(((CommandYarnClient) yarnClient), yarnClientName, appMasterProperties,
                containerProperties);
    }

    private int findAvaliablePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find an available port.", e);
        }
    }

    private String overwriteQueueInternal(String queue) {
        return LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
    }

    private void overwriteAMQueueAssignment(Properties appMasterProperties) {
        String queue = (String) appMasterProperties.get(AppMasterProperty.QUEUE.name());
        if (queue != null)
            appMasterProperties.put(AppMasterProperty.QUEUE.name(), overwriteQueueInternal(queue));
    }

    private void overwriteContainerQueueAssignment(Properties containerProperties) {
        String queue = (String) containerProperties.get("QUEUE");
        if (queue != null)
            containerProperties.put("QUEUE", overwriteQueueInternal(queue));
    }
}
