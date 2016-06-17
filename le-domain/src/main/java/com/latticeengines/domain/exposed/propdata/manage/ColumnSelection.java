package com.latticeengines.domain.exposed.propdata.manage;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnSelection {

    private List<Column> columns;
    private String name;
    private String version;

    public ColumnSelection() {
    }

    public ColumnSelection(List<ExternalColumn> externalColumns) {
        List<ColumnSelection.Column> columns = new ArrayList<>();
        for (ExternalColumn externalColumn: externalColumns) {
            ColumnSelection.Column column = new ColumnSelection.Column();
            column.setColumnName(externalColumn.getDefaultColumnName());
            column.setExternalColumnId(externalColumn.getExternalColumnID());
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
        for (Column column: columns) {
            list.add(column.getColumnName());
        }
        return list;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Column {

        private String externalColumnId;
        private String columnName;

        @JsonProperty("ExternalColumnID")
        public String getExternalColumnId() {
            return externalColumnId;
        }

        @JsonProperty("ExternalColumnID")
        public void setExternalColumnId(String externalColumnId) {
            this.externalColumnId = externalColumnId;
        }

        @JsonProperty("ColumnName")
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty("ColumnName")
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
    }

    public enum Predefined implements Dimension {
        LeadEnrichment("LeadEnrichment"), //
        DerivedColumns("DerivedColumns"), //
        Model("Model"), //
        RTS("RTS");

        private final String name;
        private static Map<String, Predefined> nameMap;
        static {
            nameMap = new HashMap<>();
            for (Predefined predefined : Predefined.values()) {
                nameMap.put(predefined.getName(), predefined);
            }
        }

        Predefined(String name) {
            this.name = name;
        }

        @MetricTag(tag = "PredefinedSelection")
        public String getName() {
            return this.name;
        }

        public static Predefined fromName(String name) {
            return nameMap.get(name);
        }

        public String getJsonFileName(String version) {
            return getName() + "_" + version + ".json";
        }

        public static EnumSet<Predefined> supportedSelections = EnumSet.of(Model, DerivedColumns, RTS);

        public static Predefined getLegacyDefaultSelection() {
            return DerivedColumns;
        }

        public static Predefined getDefaultSelection() { return RTS; }

        public static List<String> getNames() {
            return new ArrayList<>(nameMap.keySet());
        }

    }

}
