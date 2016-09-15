package com.latticeengines.domain.exposed.propdata.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

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

    public enum Predefined implements Dimension {
        LeadEnrichment("LeadEnrichment"), //
        Enrichment("Enrichment"), //
        DerivedColumns("DerivedColumns"), //
        Model("Model"), //
        DNB("DNB"), //
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

        public static EnumSet<Predefined> supportedSelections = EnumSet.of(Model, DerivedColumns, RTS, DNB);

        public static Predefined getLegacyDefaultSelection() {
            return DerivedColumns;
        }

        public static Predefined getDefaultSelection() {
            return RTS;
        }

        public static List<String> getNames() {
            return new ArrayList<>(nameMap.keySet());
        }

    }
}
