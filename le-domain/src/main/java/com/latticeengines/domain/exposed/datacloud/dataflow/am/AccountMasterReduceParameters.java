package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class AccountMasterReduceParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private Boolean hasSqlPresence = true;
    private Date timestamp;
    private List<SourceColumn> sourceColumns;

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

    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(List<SourceColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }
}
