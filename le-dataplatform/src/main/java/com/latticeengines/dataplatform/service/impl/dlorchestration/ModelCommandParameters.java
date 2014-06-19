package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Collections;
import java.util.List;

public class ModelCommandParameters {
    
    // Hardcoded name of hdfs subfolder
    public static final String EVENT_METADATA = "EventMetadata";
    
    // Mandatory parameters
    public static final String EVENT_TABLE = "EventTable";
    public static final String KEY_COLS = "KeyCols";
    public static final String MODEL_NAME = "ModelName";
    public static final String MODEL_TARGETS = "ModelTargets";
    public static final String EXCLUDE_COLUMNS = "ExcludeColumns";    
    
    // Optional parameters
    public static final String NUM_SAMPLES = "NumSamples";
    public static final String DEPIVOTED_EVENT_TABLE = "DepivotedEventTable";
    public static final String ALGORITHM_PROPERTIES = "AlgorithmProperties";
    
    private String eventTable = null;
    private String depivotedEventTable = null;
    private String metadataTable = EVENT_METADATA;
    private List<String> keyCols = Collections.emptyList();
    private int numSamples = 1; // 1 is default
    private String modelName = null;
    private List<String> modelTargets = Collections.emptyList();
    private List<String> excludeColumns = Collections.emptyList();
    private String algorithmProperties = null;

    public String getEventTable() {
        return eventTable;
    }

    public void setEventTable(String eventTable) {
        this.eventTable = eventTable;
    }

    public String getDepivotedEventTable() {
        return depivotedEventTable;
    }

    public void setDepivotedEventTable(String depivotedEventTable) {
        this.depivotedEventTable = depivotedEventTable;
    }

    public String getMetadataTable() {
        return metadataTable;
    }

    public List<String> getKeyCols() {
        return keyCols;
    }

    public void setKeyCols(List<String> keyCols) {
        this.keyCols = keyCols;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int numSamples) {
        this.numSamples = numSamples;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public List<String> getModelTargets() {
        return modelTargets;
    }

    public void setModelTargets(List<String> modelTargets) {
        this.modelTargets = modelTargets;
    }

    public List<String> getExcludeColumns() {
        return excludeColumns;
    }

    public void setExcludeColumns(List<String> excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public String getAlgorithmProperties() {
        return algorithmProperties;
    }

    public void setAlgorithmProperties(String algorithmProperties) {
        this.algorithmProperties = algorithmProperties;
    }
}
