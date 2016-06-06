package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.dataflowapi.DataFlowApiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public class RunDataFlow<T extends DataFlowStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Log log = LogFactory.getLog(RunDataFlow.class);

    @Autowired
    private DataFlowApiProxy dataFlowApiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside RunDataFlow execute() [" + configuration.getBeanName() + "]");
        runDataFlow();
    }

    private void runDataFlow() {
        DataFlowConfiguration dataFlowConfig = setupDataFlow();
        AppSubmission submission = dataFlowApiProxy.submitDataFlowExecution(dataFlowConfig);
        waitForAppId(submission.getApplicationIds().get(0), configuration.getMicroServiceHostPort());
    }

    private DataFlowConfiguration setupDataFlow() {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setTargetTableName(configuration.getTargetTableName());
        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());
        dataFlowConfig.setDataFlowBeanName(configuration.getBeanName());
        dataFlowConfig.setDataSources(createDataFlowSources(configuration.getDataFlowParams()));
        dataFlowConfig.setDataFlowParameters(configuration.getDataFlowParams());
        return dataFlowConfig;
    }

    private List<DataFlowSource> createDataFlowSources(DataFlowParameters parameters) {
        List<DataFlowSource> sources = new ArrayList<>();
        Set<String> sourceNames = parameters != null ? parameters.getSourceTableNames() : new HashSet<String>();
        if (sourceNames.isEmpty()) {
            // May be missing an extra source
            sourceNames = new HashSet<>(metadataProxy.getTableNames(configuration.getCustomerSpace().toString()));
        }

        for (String name : sourceNames) {
            if (configuration.getExtraSources().containsKey(name)) {
                registerTable(name, configuration.getExtraSources().get(name));
            }
            DataFlowSource source = new DataFlowSource();
            source.setName(name);
            sources.add(source);
        }

        // Go through the extra sources and make sure that all are
        // registered and provided
        for (final String extraSourceName : configuration.getExtraSources().keySet()) {
            DataFlowSource extraSource = Iterables.find(sources, new Predicate<DataFlowSource>() {
                @Override
                public boolean apply(@Nullable DataFlowSource source) {
                    return source.getName().equals(extraSourceName);
                }
            }, null);
            if (extraSource == null) {
                registerTable(extraSourceName, configuration.getExtraSources().get(extraSourceName));
                DataFlowSource source = new DataFlowSource();
                source.setName(extraSourceName);
                sources.add(source);
            }
        }

        for (DataFlowSource source : sources) {
            log.info(String.format("Providing source %s to data flow %s", source.getName(), configuration.getBeanName()));
        }
        return sources;
    }

    private void registerTable(String name, String path) {
        Table table = MetadataConverter.getTable(yarnConfiguration, path, null, null);
        table.setName(name);
        if (metadataProxy.getTable(configuration.getCustomerSpace().toString(), table.getName()) == null) {
            metadataProxy.createTable(configuration.getCustomerSpace().toString(), table.getName(), table);
        }
    }
}
