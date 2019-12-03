package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class AccountMasterRebuildParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private String[] joinFields;
    private String[] seedFields;
    private Boolean hasSqlPresence = true;
    private Date timestamp;
    private List<String> id;
    private String domainKey;
    private String dunsKey;
    private List<SourceColumn> sourceColumns;
    private List<AccountMasterSourceParameters> sourceParameters;
    private String lastSource;

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

    public List<String> getId() {
        return id;
    }

    public void setId(List<String> id) {
        this.id = id;
    }

    public String getDomainKey() {
        return domainKey;
    }

    public void setDomainKey(String domainKey) {
        this.domainKey = domainKey;
    }

    public String getDunsKey() {
        return dunsKey;
    }

    public void setDunsKey(String dunsKey) {
        this.dunsKey = dunsKey;
    }

    public List<SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(List<SourceColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public List<AccountMasterSourceParameters> getSourceParameters() {
        return sourceParameters;
    }

    public void setSourceParameters(List<AccountMasterSourceParameters> sourceParameters) {
        this.sourceParameters = sourceParameters;
    }

    public String getLastSource() {
        return lastSource;
    }

    public void setLastSource(String lastSource) {
        this.lastSource = lastSource;
    }

    public String[] getSeedFields() {
        return seedFields;
    }

    public void setSeedFields(String[] seedFields) {
        this.seedFields = seedFields;
    }
}
