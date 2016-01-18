package com.latticeengines.domain.exposed.propdata;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnSelection {

    private List<Column> columns;
    private String name;
    private String version;

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

    public enum Predefined {
        LEAD_ENRICHMENT("LeadEnrichment"), DERIVED_COLUMNS("DerivedColumns");

        private final String name;
        private static Map<String, Predefined> nameMap;

        static {
            nameMap = new HashMap<>();
            for (Predefined predefined: Predefined.values()) {
                nameMap.put(predefined.getName(), predefined);
            }
        }

        Predefined(String name) {
            this.name = name;
        }

        public String getName() { return this.name; }

        public static Predefined fromName(String name) {
            return nameMap.get(name);
        }

        public String getJsonFileName(String version) {
            return getName() + "_" + version + ".json";
        }

    }


}
