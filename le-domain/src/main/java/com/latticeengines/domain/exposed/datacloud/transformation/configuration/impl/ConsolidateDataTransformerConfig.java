package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Set;

public class ConsolidateDataTransformerConfig extends TransformerConfig {

    private String srcIdField;
    private String masterIdField;

    private boolean createTimestampColumn;

    private Set<String> columnsFromRight;
    private List<String> compositeKeys;
    private boolean dedupeSource;
    private boolean addTimestamps;
    private boolean mergeOnly;

    public String getSrcIdField() {
        return srcIdField;
    }

    public void setSrcIdField(String srcIdField) {
        this.srcIdField = srcIdField;
    }

    public String getMasterIdField() {
        return masterIdField;
    }

    public void setMasterIdField(String masterIdField) {
        this.masterIdField = masterIdField;
    }

    public boolean isCreateTimestampColumn() {
        return createTimestampColumn;
    }

    public void setCreateTimestampColumn(boolean createTimestampColumn) {
        this.createTimestampColumn = createTimestampColumn;
    }

    public Set<String> getColumnsFromRight() {
        return columnsFromRight;
    }

    public void setColumnsFromRight(Set<String> columnsFromRight) {
        this.columnsFromRight = columnsFromRight;
    }

    public List<String> getCompositeKeys() {
        return compositeKeys;
    }

    public void setCompositeKeys(List<String> compositeKeys) {
        this.compositeKeys = compositeKeys;
    }

    public boolean isDedupeSource() {
        return dedupeSource;
    }

    public void setDedupeSource(boolean isDedupeSource) {
        this.dedupeSource = isDedupeSource;
    }

    public boolean isAddTimestamps() {
        return addTimestamps;
    }

    public void setAddTimestamps(boolean addTimestamps) {
        this.addTimestamps = addTimestamps;
    }

    public boolean isMergeOnly() {
        return mergeOnly;
    }

    public void setMergeOnly(boolean mergeOnly) {
        this.mergeOnly = mergeOnly;
    }
}
