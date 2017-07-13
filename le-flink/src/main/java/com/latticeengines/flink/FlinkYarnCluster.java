package com.latticeengines.flink;

import static com.latticeengines.flink.FlinkConstants.JM_HEAP_CONF;
import static com.latticeengines.flink.FlinkConstants.NUM_CONTAINERS;
import static com.latticeengines.flink.FlinkConstants.TM_HEAP_CONF;
import static com.latticeengines.flink.FlinkConstants.TM_SLOTS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.latticeengines.common.exposed.util.YarnUtils;

public class FlinkYarnCluster {

    private static final Logger log = LoggerFactory.getLogger(FlinkYarnCluster.class);

    private static YarnClusterClient yarnCluster;
    private static ContextEnvironment executionEnvironment;
    private static int parallelism = 1;
    private static YarnConfiguration yarnConf;
    private static Configuration flinkConf;

    private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this
                                                                          // has
                                                                          // to
                                                                          // be
                                                                          // a
                                                                          // regex
                                                                          // for
                                                                          // String.split()

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

        int numBuffers = Math.max(tmSlots * tmSlots * numContainers * 4, 1024);
        setConfIfNotAlready(flinkConf, "taskmanager.network.numberOfBuffers", String.valueOf(numBuffers));
        setConfIfNotAlready(flinkConf, "akka.ask.timeout", "15s");
        setConfIfNotAlready(flinkConf, "restart-strategy", "fixed-delay");
        setConfIfNotAlready(flinkConf, "restart-strategy.fixed-delay.attempts", "10");

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

        List<String> dynamicOpts = new ArrayList<>();
        flinkConf.toMap().entrySet().forEach(entry -> {
            dynamicOpts.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        });
        yarnClusterDescriptor
                .setDynamicPropertiesEncoded(StringUtils.join(dynamicOpts, YARN_DYNAMIC_PROPERTIES_SEPARATOR));

        FlinkYarnCluster.yarnConf = yarnConf;
        FlinkYarnCluster.flinkConf = flinkConf;

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
            executionEnvironment = new ContextEnvironment(yarnCluster, Collections.emptyList(), Collections.emptyList(),
                    Thread.currentThread().getContextClassLoader(), null);
            executionEnvironment.setParallelism(parallelism);

            Map<String, String> jobParams = new HashMap<>();
            yarnConf.forEach((entry) -> {
                jobParams.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            });
            flinkConf.toMap().entrySet().forEach((entry) -> {
                jobParams.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            });
            ParameterTool params = ParameterTool.fromMap(jobParams);
            executionEnvironment.getConfig().setGlobalJobParameters(params);
        }
        return executionEnvironment;
    }

    public static void shutdown() {
        if (yarnCluster != null) {
            log.info("Shutting down the yarnCluster");
            yarnCluster.shutdownCluster();
            try {
                ApplicationId appId = yarnCluster.getApplicationId();
                ApplicationReport applicationReport = YarnUtils.getApplicationReport(yarnConf, appId);
                FinalApplicationStatus status = applicationReport.getFinalApplicationStatus();
                if (!YarnUtils.TERMINAL_STATUS.contains(status)) {
                    YarnUtils.kill(yarnConf, appId);
                }
            } catch (Exception e) {
                log.error("Failed to shutdown yarn application", e);
            }
        }
    }

    private static void setConfIfNotAlready(Configuration flinkConf, String key, String value) {
        if (!flinkConf.containsKey(key)) {
            flinkConf.setString(key, value);
        }
    }

}
