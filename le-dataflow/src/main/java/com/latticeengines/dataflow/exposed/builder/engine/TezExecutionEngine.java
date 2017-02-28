package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;

public class TezExecutionEngine extends ExecutionEngine {

    private static final Log log  = LogFactory.getLog(TezExecutionEngine.class);

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
        properties = updateTezRuntime(properties);
        return new Hadoop2TezFlowConnector(properties);
    }

    private Properties updateTezRuntime(Properties properties) {
        if (StringUtils.isNotBlank(properties.getProperty("tez.task.resource.memory.mb"))
                && StringUtils.isBlank(properties.getProperty("tez.runtime.io.sort.mb"))) {
            long taskmb = Long.valueOf(properties.getProperty("tez.task.resource.memory.mb"));
            long sortmb = Math.min(Math.round(taskmb * 0.5), 2048);
            properties.put("tez.runtime.io.sort.mb", String.valueOf(sortmb));
            log.info("Automatically set tez.runtime.io.sort.mb = " + properties.get("tez.runtime.io.sort.mb"));
        }
        return properties;
    }

}
