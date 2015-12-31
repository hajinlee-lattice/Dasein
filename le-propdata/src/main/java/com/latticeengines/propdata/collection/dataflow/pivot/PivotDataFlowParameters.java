package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.List;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;

public class PivotDataFlowParameters extends DataFlowParameters {

    private String timestampField;
    private List<String> baseTables;
    private String[] joinFields;
    private List<SourceColumn> columns;

    public String getTimestampField() { return timestampField; }

    public void setTimestampField(String timestampField) { this.timestampField = timestampField; }

    public List<String> getBaseTables() { return baseTables; }

    public void setBaseTables(List<String> baseTables) { this.baseTables = baseTables; }

    public List<SourceColumn> getColumns() { return columns; }

    public void setColumns(List<SourceColumn> columns) { this.columns = columns; }

    public String[] getJoinFields() { return joinFields; }

    public void setJoinFields(String[] joinFields) { this.joinFields = joinFields; }
}
