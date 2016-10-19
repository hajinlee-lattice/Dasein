package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DepivotDataFlowParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private String[] joinFields;
    private List<SourceColumn> columns;
    private List<List<SourceColumn>> baseSourceColumns;
    private Boolean hasSqlPresence = true;
    private Date timestamp;

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public List<String> getBaseTables() {
        return baseTables;
    }

    public void setBaseTables(List<String> baseTables) {
        this.baseTables = baseTables;
    }

    public List<SourceColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<SourceColumn> columns) {
        this.columns = columns;
    }

    public String[] getJoinFields() {
        return joinFields;
    }

    public void setJoinFields(String[] joinFields) {
        this.joinFields = joinFields;
    }

    public Boolean hasSqlPresence() {
        return hasSqlPresence;
    }

    public void setHasSqlPresence(Boolean hasSqlPresence) {
        this.hasSqlPresence = hasSqlPresence;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public List<List<SourceColumn>> getBaseSourceColumns() {
        return baseSourceColumns;
    }

    public void setBaseSourceColumns(List<List<SourceColumn>> baseSourceColumns) {
        this.baseSourceColumns = baseSourceColumns;
    }
}
