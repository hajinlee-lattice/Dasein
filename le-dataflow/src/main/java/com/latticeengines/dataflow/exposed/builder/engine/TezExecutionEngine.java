package com.latticeengines.dataflow.exposed.builder.engine;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.dataflow.exposed.builder.ExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;

public class TezExecutionEngine extends ExecutionEngine {
    private static final String TEZ_AM_MEMORY_PROPERTY = "tez.am.resource.memory.mb";

    private static final int DEFAULT_TEZ_AM_MEMORY_MB = 2048;

    private static final Logger log  = LoggerFactory.getLogger(TezExecutionEngine.class);

    public TezExecutionEngine() {
        setName("TEZ");
        setDefault(true);
        register(this);
    }

    @Override
    public FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties) {
        Configuration config = dataFlowCtx.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);
        properties.put("tez.queue.name", getQueue(dataFlowCtx));
        for (Map.Entry<String, String> entry : config) {
            properties.put(entry.getKey(), entry.getValue());
        }
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(getPartitions(dataFlowCtx))
                .buildProperties(properties);
        properties = updateTezRuntime(config, properties);
        return new Hadoop2TezFlowConnector(properties);
    }

    private Properties updateTezRuntime(Configuration config, Properties properties) {
        if (StringUtils.isNotBlank(properties.getProperty("tez.task.resource.memory.mb"))
                && StringUtils.isBlank(properties.getProperty("tez.runtime.io.sort.mb"))) {
            long taskmb = Long.valueOf(properties.getProperty("tez.task.resource.memory.mb"));
            long sortmb = Math.min(Math.round(taskmb * 0.5), 2048);
            properties.put("tez.runtime.io.sort.mb", String.valueOf(sortmb));
            log.info("Automatically set tez.runtime.io.sort.mb = " + properties.get("tez.runtime.io.sort.mb"));
        }
        if (StringUtils.isBlank(properties.getProperty(TEZ_AM_MEMORY_PROPERTY))) {
            // request default memory if it is not set
            properties.put(TEZ_AM_MEMORY_PROPERTY, String.valueOf(DEFAULT_TEZ_AM_MEMORY_MB));
        }
        properties.put("tez.runtime.unordered.output.buffer.size-mb", "512");
        properties.put("tez.lib.uris",
                config.get(FileSystem.FS_DEFAULT_NAME_KEY) + PropertyUtils.getProperty("tez.lib.uris"));
        return properties;
    }

}
