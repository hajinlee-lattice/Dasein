package com.latticeengines.flink;

import static com.latticeengines.flink.FlinkConstants.JM_HEAP_CONF;
import static com.latticeengines.flink.FlinkConstants.NUM_CONTAINERS;
import static com.latticeengines.flink.FlinkConstants.TM_HEAP_CONF;
import static com.latticeengines.flink.FlinkConstants.TM_SLOTS;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class FlinkYarnCluster {

    private static final Log log = LogFactory.getLog(FlinkYarnCluster.class);

    private static YarnClusterClient yarnCluster;
    private static ContextEnvironment executionEnvironment;
    private static int parallelism = 1;

    public static AbstractYarnClusterDescriptor createDescriptor(YarnConfiguration yarnConf, Configuration flinkConf,
            String appName, String queue) {
        if (flinkConf == null) {
            flinkConf = new Configuration();
        }

        Integer numContainers = Integer.valueOf(flinkConf.getString(NUM_CONTAINERS, "1"));
        Integer tmSlots = Integer.valueOf(flinkConf.getString(TM_SLOTS, "1"));
        Integer tmMem = Integer.valueOf(flinkConf.getString(TM_HEAP_CONF, "1024"));
        Integer jmMem = Integer.valueOf(flinkConf.getString(JM_HEAP_CONF, "1024"));
        parallelism = tmSlots * numContainers;

        flinkConf.setString(JM_HEAP_CONF, String.valueOf(jmMem));
        flinkConf.setString(TM_HEAP_CONF, String.valueOf(tmMem));
        flinkConf.setString(TM_SLOTS, String.valueOf(tmSlots));
        flinkConf.setString(NUM_CONTAINERS, String.valueOf(numContainers));

        AbstractYarnClusterDescriptor yarnClusterDescriptor = new FlinkYarnClusterDescriptor(yarnConf);
        // queue
        yarnClusterDescriptor.setQueue(queue);

        yarnClusterDescriptor.setTaskManagerCount(numContainers);
        yarnClusterDescriptor.setTaskManagerSlots(tmSlots);
        yarnClusterDescriptor.setTaskManagerMemory(tmMem);
        yarnClusterDescriptor.setJobManagerMemory(jmMem);

        if (StringUtils.isNotBlank(appName)) {
            yarnClusterDescriptor.setName(appName);
        }

        String message = String.format(
                "The YARN cluster has %d TaskManagers, with %d slots on each of them, so %d slots in total.",
                yarnClusterDescriptor.getTaskManagerCount(), yarnClusterDescriptor.getTaskManagerSlots(), parallelism);
        log.info(message);

        yarnClusterDescriptor.setFlinkConfiguration(flinkConf);

        File configFile = new File("./flink-conf.yaml");
        try {
            FileUtils.touch(configFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create empty flink configuration file", e);
        }
        yarnClusterDescriptor.setConfigurationDirectory(new File(configFile.getAbsolutePath()).getParent());
        yarnClusterDescriptor.setConfigurationFilePath(new Path(configFile.getPath()));

        return yarnClusterDescriptor;
    }

    public static void launch(AbstractYarnClusterDescriptor yarnDescriptor) {
        try {
            yarnCluster = yarnDescriptor.deploy();
        } catch (Exception e) {
            throw new RuntimeException("Error while deploying YARN cluster", e);
        }
        // ------------------ ClusterClient deployed, handle connection details
        String jobManagerAddress = yarnCluster.getJobManagerAddress().getAddress().getHostName() + ":"
                + yarnCluster.getJobManagerAddress().getPort();

        System.out.println("Flink JobManager is now running on " + jobManagerAddress);
        System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());

        log.info("The Flink YARN cluster [" + yarnCluster.getApplicationId() + "] has been started in detached mode.");
        yarnCluster.waitForClusterToBeReady();
        Runtime.getRuntime().addShutdownHook(new Thread(FlinkYarnCluster::shutdown));
    }

    public static YarnClusterClient getClient() {
        return yarnCluster;
    }

    public static ContextEnvironment getExecutionEnvironment() {
        if (executionEnvironment == null) {
            String host = yarnCluster.getJobManagerAddress().getAddress().getHostName();
            int port = yarnCluster.getJobManagerAddress().getPort();
            executionEnvironment = new ContextEnvironment(yarnCluster, Collections.emptyList(), Collections.emptyList(),
                    Thread.currentThread().getContextClassLoader(), null);
            executionEnvironment.setParallelism(parallelism);
        }
        return executionEnvironment;
    }

    public static void shutdown() {
        if (yarnCluster != null) {
            log.info("Shutting down the yarnCluster");
            yarnCluster.shutdownCluster();
        }
    }

}
