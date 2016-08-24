package com.latticeengines.domain.exposed.propdata.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnSelection {

    private List<Column> columns;
    private String name;
    private String version;

    public ColumnSelection() {
    }

    public void createColumnSelection(List<ExternalColumn> externalColumns) {
        List<Column> columns = new ArrayList<>();
        for (ExternalColumn externalColumn : externalColumns) {
            Column column = new Column();
            column.setExternalColumnId(externalColumn.getExternalColumnID());
            column.setColumnName(externalColumn.getDefaultColumnName());
            columns.add(column);
        }
        setColumns(columns);
    }

    public void createAccountMasterColumnSelection(List<AccountMasterColumn> accountMasterColumns) {
        List<Column> columns = new ArrayList<>();
        for (AccountMasterColumn accountMasterColumn : accountMasterColumns) {
            Column column = new Column();
            column.setExternalColumnId(accountMasterColumn.getColumnId());
            column.setColumnName(accountMasterColumn.getDisplayName());
            columns.add(column);
        }
        setColumns(columns);
    }

    @JsonProperty("Columns")
    public List<Column> getColumns() {
        return columns;
    }

    @JsonProperty("Columns")
    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    @JsonProperty("Name")
    public String getName() {
        return name;
    }

    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("Version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("Version")
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonIgnore
    public List<String> getColumnNames() {
        List<String> list = new ArrayList<>();
        for (Column column : columns) {
            list.add(column.getColumnName());
        }
        return list;
    }

    public static ColumnSelection combine(Collection<ColumnSelection> selectionCollection) {
        Set<Column> columns = new HashSet<>();
        for (ColumnSelection selection : selectionCollection) {
            columns.addAll(selection.getColumns());
        }
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(new ArrayList<Column>(columns));
        return columnSelection;
    }

    @JsonIgnore
    public Boolean isEmpty() {
        return getColumns().isEmpty();
    }

}
