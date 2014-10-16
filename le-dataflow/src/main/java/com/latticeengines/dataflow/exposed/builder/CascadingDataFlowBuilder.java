package com.latticeengines.dataflow.exposed.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.util.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;

public abstract class CascadingDataFlowBuilder {

    private boolean local = false;
    
    @SuppressWarnings("rawtypes")
    private Map<String, Tap> taps = new HashMap<>();
    
    private Map<String, Schema> schemas = new HashMap<>();
    
    private Map<String, Pair<Pipe, Schema>> pipesAndOutputSchemas = new HashMap<>();
    
    public CascadingDataFlowBuilder() {
        this(false);
    }
    
    public CascadingDataFlowBuilder(boolean local) {
        this.local = local;
    }
    
    public abstract FlowDef constructFlowDefinition(Map<String, String> sources);
    
    public Flow<?> build(FlowDef flowDef) {
        return null;
    }
    
    protected void addSource(String sourceName, String sourcePath) {
        Tap<?, ?, ?> tap = new Hfs(new AvroScheme(), sourcePath);
        if (local) {
            tap = new Lfs(new AvroScheme(), sourcePath);
        }
        
        taps.put(sourceName, tap);
        
        Schema sourceSchema = AvroUtils.getSchema(new Configuration(), new Path(sourcePath));
        schemas.put(sourceName, sourceSchema);
    }
    
    protected List<String> getFieldNames(String sourceName) {
        return null;
    }
    
    protected String addInnerJoin(Map<String, Pair<List<String>, List<String>>> joinKeyAndOutputs) {
        return null;
    }
    
    protected String addGroupBy(String prior, List<String> groupByFields) {
        return null;
    }
    
    protected String addTarget(String targetName, Map<String, List<String>> targetFieldNames) {
        return null;
    }
    
    protected String addFilter(String prior, String expression) {
        return null;
    }
    
    protected String addFunction(String prior) {
        return null;
    }
}
