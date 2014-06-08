package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Collections;
import java.util.List;

public class ModelCommandParameters {
    
    public static final String EVENT_TABLE = "EventTable";
    public static final String DEPIVOTED_EVENT_TABLE = "DepivotedEventTable";
    public static final String METADATA_TABLE = "MetadataTable";
    public static final String KEY_COLS = "KeyCols";
    public static final String MODEL_NAME = "ModelName";
    public static final String MODEL_TARGETS = "ModelTargets";
    
    // OPTIONAL
    public static final String NUM_SAMPLES = "NumSamples";
    
    private String eventTable;
    private String depivotedEventTable;
    private String metadataTable;
    private List<String> keyCols = Collections.emptyList();
    private int numSamples = 1; // 1 is default
    private String modelName;
    private List<String> modelTargets = Collections.emptyList();

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

    public void setMetadataTable(String metadataTable) {
        this.metadataTable = metadataTable;
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
}
