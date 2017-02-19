package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import org.apache.flink.api.java.ExecutionEnvironment;

import com.dataartisans.flink.cascading.FlinkConnector;
import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;

public class FlinkExecutionEngine extends ExecutionEngine {

    public FlinkExecutionEngine() {
        setName("FLINK");
        setDefault(false);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(getPartitions(dataFlowCtx))
                .buildProperties(properties);
        ExecutionEnvironment environment = dataFlowCtx.getProperty(DataFlowProperty.FLINKENV,
                ExecutionEnvironment.class);
        if (environment == null) {
            environment = ExecutionEnvironment.getExecutionEnvironment();
        }
        return new FlinkConnector(environment, properties);
    }

}
