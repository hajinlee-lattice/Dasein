package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Set;

public class ConsolidateDataTransformerConfig extends TransformerConfig {

    private String srcIdField;
    private String masterIdField;

    private boolean createTimestampColumn;

    private Set<String> columnsFromRight;
    private List<String> compositeKeys;
    private List<String> origMasterFields;

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

    public List<String> getOrigMasterFields() {
        return origMasterFields;
    }

    public void setOrigMasterFields(List<String> origMasterFields) {
        this.origMasterFields = origMasterFields;
    }

}
