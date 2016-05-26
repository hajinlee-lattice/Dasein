package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;

public class TezExecutionEngine extends ExecutionEngine {

    public TezExecutionEngine() {
        setName("TEZ");
        setDefault(true);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        properties.put("tez.queue.name", getQueue(dataFlowCtx));
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(getPartitions(dataFlowCtx))
                .buildProperties(properties);
        return new Hadoop2TezFlowConnector(properties);
    }

}
