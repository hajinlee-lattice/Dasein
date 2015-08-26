package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;

import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class MapReduceExecutionEngine extends ExecutionEngine {
    
    public MapReduceExecutionEngine() {
        setName("MR");
        setDefault(true);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        String queue = getQueue(dataFlowCtx);
        properties.put("mapred.job.queue.name", queue);
        return new HadoopFlowConnector(properties);
    }

    @Override
    public boolean isDefault() {
        return true;
    }

}
