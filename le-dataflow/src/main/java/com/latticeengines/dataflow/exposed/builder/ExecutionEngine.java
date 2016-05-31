package com.latticeengines.dataflow.exposed.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.builder.engine.FlinkExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.engine.MapReduceExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.engine.TezExecutionEngine;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import cascading.flow.FlowConnector;

public abstract class ExecutionEngine {

    private String name;
    private boolean isDefault;
    protected boolean enforceGlobalOrdering;
    private static Map<String, ExecutionEngine> engineRegistry = new HashMap<>();
    
    static {
        new MapReduceExecutionEngine();
        new TezExecutionEngine();
        new FlinkExecutionEngine();
    }
    
    protected static void register(ExecutionEngine engine) {
        engineRegistry.put(engine.getName(), engine);
    }
    
    public static ExecutionEngine get(String engineType) {
        ExecutionEngine engine = engineRegistry.get(engineType);
        if (engine == null) {
            for (ExecutionEngine e : engineRegistry.values()) {
                if (e.isDefault()) {
                    engine = e;
                    break;
                }
            }
        }
        return engine;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    protected String getQueue(DataFlowContext dataFlowCtx) {
        return dataFlowCtx.getProperty(DataFlowProperty.QUEUE, String.class);
    }

    protected Integer getPartitions(DataFlowContext dataFlowCtx) {
        if (dataFlowCtx.containsProperty(DataFlowProperty.PARTITIONS)) {
            return dataFlowCtx.getProperty(DataFlowProperty.PARTITIONS, Integer.class);
        } else {
            return 1;
        }
    }
    
    public abstract FlowConnector createFlowConnector(DataFlowContext dataFlowCtx, Properties properties);
    
    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean isDefault) {
        this.isDefault = isDefault;
    }

    public void setEnforceGlobalOrdering(boolean enforceGlobalOrdering) { this.enforceGlobalOrdering = enforceGlobalOrdering; }
}
