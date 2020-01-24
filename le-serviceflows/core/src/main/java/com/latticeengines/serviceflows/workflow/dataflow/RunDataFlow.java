package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.dataflowapi.DataFlowApiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
public class RunDataFlow<T extends DataFlowStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(RunDataFlow.class);

    @Inject
    private DataFlowApiProxy dataFlowApiProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

    @Value("${pls.cdl.transform.tez.am.mem.gb}")
    private int tezAmMemGb; // requested memory for application master

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    private int scalingMultiplier = 1;

    @Override
    public void execute() {
        log.info("Inside RunDataFlow execute() [" + configuration.getBeanName() + "]");
        runDataFlow();
    }

    private void runDataFlow() {
        DataFlowConfiguration dataFlowConfig = setupDataFlow();
        log.info("dataflow configuration is {}", dataFlowConfig);
        AppSubmission submission = dataFlowApiProxy.submitDataFlowExecution(dataFlowConfig);
        waitForAppId(submission.getApplicationIds().get(0));
    }

    private DataFlowConfiguration setupDataFlow() {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setTargetTableName(configuration.getTargetTableName());
        dataFlowConfig.setTargetPath(configuration.getTargetPath());
        dataFlowConfig.setPartitions(configuration.getPartitions());
        dataFlowConfig.setEngine(configuration.getEngine());
        dataFlowConfig.setQueue(configuration.getQueue());
        dataFlowConfig.setSwlib(configuration.getSwlib());

        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());
        dataFlowConfig.setDataFlowBeanName(configuration.getBeanName());
        dataFlowConfig.setDataSources(createDataFlowSources(configuration.getDataFlowParams()));
        dataFlowConfig.setDataFlowParameters(configuration.getDataFlowParams());
        dataFlowConfig.setApplyTableProperties(configuration.isApplyTableProperties());

        dataFlowConfig.setAmMemGb(getYarnAmMemGb());
        dataFlowConfig.setAmVcores(getYarnAmVCores());

        setJobProperties(dataFlowConfig);

        return dataFlowConfig;
    }

    private void setJobProperties(DataFlowConfiguration dataFlowConfig) {
        if (configuration.getJobProperties() == null) {
            Properties jobProperties = initJobProperties();
            int partitions = cascadingPartitions * scalingMultiplier;
            jobProperties.put("mapreduce.job.reduces", String.valueOf(partitions));
            jobProperties.put("mapred.reduce.tasks", String.valueOf(partitions));
            jobProperties.put("mapreduce.job.running.map.limit", "200");
            jobProperties.put("mapreduce.job.running.reduce.limit", "100");
            dataFlowConfig.setJobProperties(jobProperties);
            dataFlowConfig.setPartitions(partitions);
        } else {
            dataFlowConfig.setJobProperties(configuration.getJobProperties());
        }
    }

    protected Properties initJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024));
        return jobProperties;
    }

    private List<DataFlowSource> createDataFlowSources(DataFlowParameters parameters) {
        List<DataFlowSource> sources = new ArrayList<>();
        Set<String> sourceNames = parameters != null ? parameters.getSourceTableNames() : new HashSet<>();
        if (sourceNames.isEmpty()) {
            // May be missing an extra source
            sourceNames = new HashSet<>(metadataProxy.getTableNames(configuration.getCustomerSpace().toString()));
        }

        // Go through the extra sources and make sure that all are
        // registered and provided
        double maxSizeInGb = 0.0;
        for (String name : sourceNames) {
            double sizeInGb = 0.0;
            if (configuration.getExtraSources().containsKey(name)) {
                sizeInGb = registerTable(name, configuration.getExtraSources().get(name));
            } else {
                Table table = metadataProxy.getTableSummary(configuration.getCustomerSpace().toString(), name);
                sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, table);
                log.info("Found size=" + sizeInGb + " gb for source " + table.getName());
            }
            DataFlowSource source = new DataFlowSource();
            source.setName(name);
            sources.add(source);
            maxSizeInGb += sizeInGb;
        }

        for (final String extraSourceName : configuration.getExtraSources().keySet()) {
            DataFlowSource extraSource = sources.stream() //
                    .filter(source -> source.getName().equals(extraSourceName)) //
                    .findFirst().orElse(null);
            if (extraSource == null) {
                double sizeInGb = registerTable(extraSourceName, configuration.getExtraSources().get(extraSourceName));
                DataFlowSource source = new DataFlowSource();
                source.setName(extraSourceName);
                sources.add(source);
                maxSizeInGb += sizeInGb;
            }
        }
        scalingMultiplier = getScalingMultiplier(maxSizeInGb);
        for (DataFlowSource source : sources) {
            log.info(String.format("Providing source %s to data flow %s", source.getName(),
                    configuration.getBeanName()));
        }
        return sources;
    }

    private double registerTable(String name, String path) {
        Table table = MetadataConverter.getTable(yarnConfiguration, path, null, null);
        table.setName(name);
        if (metadataProxy.getTable(configuration.getCustomerSpace().toString(), table.getName()) == null) {
            metadataProxy.createTable(configuration.getCustomerSpace().toString(), table.getName(), table);
        }
        double sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, table);
        log.info("Found size=" + sizeInGb + " gb for table " + table.getName());
        return sizeInGb;
    }

    private int getScalingMultiplier(double sizeInGb) {
        int multiplier = ScalingUtils.getMultiplier(sizeInGb);
        if (multiplier > 1) {
            log.info("Set multiplier=" + multiplier + " base on size=" + sizeInGb + " gb.");
        }
        return multiplier;
    }

    protected Integer getYarnAmMemGb() {
        return null;
    }

    protected Integer getYarnAmVCores() {
        return null;
    }

}
