package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import com.dataartisans.flink.cascading.FlinkConnector;
import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;

public class FlinkExecutionEngine  extends ExecutionEngine {

    public FlinkExecutionEngine() {
        setName("FLINK");
        setDefault(false);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(getPartitions(dataFlowCtx))
                .buildProperties(properties);
        return new FlinkConnector(properties);
    }

}
