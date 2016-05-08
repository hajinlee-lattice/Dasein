package com.latticeengines.domain.exposed.propdata.dataflow;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

public class DepivotDataFlowParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private String[] joinFields;
    private List<SourceColumn> columns;
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
}
