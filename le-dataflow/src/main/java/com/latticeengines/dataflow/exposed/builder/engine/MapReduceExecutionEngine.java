package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
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
        String queue = getQueue(dataFlowCtx);
        properties.put("mapreduce.job.queuename", queue);
        if (enforceGlobalOrdering) {
            properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(1).buildProperties(properties);
        }
        return new HadoopFlowConnector(properties);
    }

    @Override
    public boolean isDefault() {
        return false;
    }

}
