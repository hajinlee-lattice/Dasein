package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop.HadoopFlowConnector;

public class MapReduceExecutionEngine extends ExecutionEngine {

    public MapReduceExecutionEngine() {
        setName("MR");
        setDefault(false);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        Configuration config = dataFlowCtx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);
        for (Map.Entry<String, String> entry : config) {
            properties.put(entry.getKey(), entry.getValue());
        }
        String queue = getQueue(dataFlowCtx);
        properties.put("mapreduce.job.queuename", queue);
        if (enforceGlobalOrdering) {
            properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(1).buildProperties(properties);
        } else {
            properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(getPartitions(dataFlowCtx))
                    .buildProperties(properties);
        }
        return new HadoopFlowConnector(properties);
    }

    @Override
    public boolean isDefault() {
        return false;
    }

}
