package com.latticeengines.domain.exposed.propdata.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static ColumnSelection combine(Collection<ColumnSelection> selectionCollection) {
        List<Column> columns = new ArrayList<>();
        for (ColumnSelection selection : selectionCollection) {
            if (columns.isEmpty()) {
                columns.addAll(selection.getColumns());
            } else {
                List<Column> incremental = new ArrayList<>();
                selection.getColumns().forEach(col -> {
                    if (!columns.contains(col)) {
                        incremental.add(col);
                    }
                });
                columns.addAll(incremental);
            }
        }
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        return columnSelection;
    }

    public void createColumnSelection(List<ExternalColumn> externalColumns) {
        List<Column> columns = new ArrayList<>();
        for (ExternalColumn externalColumn : externalColumns) {
            Column column = new Column();
            column.setExternalColumnId(externalColumn.getExternalColumnID());
            column.setColumnName(externalColumn.getExternalColumnID());
            columns.add(column);
        }
        setColumns(columns);
    }

    public void createAccountMasterColumnSelection(List<AccountMasterColumn> accountMasterColumns) {
        List<Column> columns = new ArrayList<>();
        for (AccountMasterColumn accountMasterColumn : accountMasterColumns) {
            Column column = new Column();
            column.setExternalColumnId(accountMasterColumn.getColumnId());
            column.setColumnName(accountMasterColumn.getColumnId());
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

    @JsonIgnore
    public List<String> getColumnIds() {
        List<String> list = new ArrayList<>();
        for (Column column : columns) {
            list.add(column.getExternalColumnId());
        }
        return list;
    }

    @JsonIgnore
    public Boolean isEmpty() {
        return getColumns().isEmpty();
    }

    public enum Predefined implements Dimension {
        // DataCloud
        LeadEnrichment("LeadEnrichment"), //
        Enrichment("Enrichment"), //
        DerivedColumns("DerivedColumns"), //
        Model("Model"), //
        Segment("Segment"), //
        ID("ID"), // Shared in entity match
        RTS("RTS"),

        // CDL
        LookupId("LookupId"), //
        TalkingPoint("TalkingPoint"), //
        CompanyProfile("CompanyProfile"), //

        // Entity Match

        // Return attrs from entity seed table. Currently only returns
        // LatticeAccountId. Could add more if necessary, but need to deal with
        // issue that attr name conflicts with match input
        Seed("Seed"), //
        // Only for M27 branched feature of Lead-to-Account match to return
        // AccountId from Account seed table for Contact. Might be removed in
        // future.
        LeadToAcct("LeadToAcct"), //
        ;

        public static final String[] usageProperties = { ColumnSelection.Predefined.Segment.getName(),
                ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.Model.getName(),
                ColumnSelection.Predefined.TalkingPoint.getName(),
                ColumnSelection.Predefined.CompanyProfile.getName() };

        // For DataCloud match & CDL match (before M25) & attribute lookup in entity
        // match
        public static final EnumSet<Predefined> supportedSelections = EnumSet.of(Model, DerivedColumns, RTS, ID,
                Enrichment, Segment, TalkingPoint, CompanyProfile, Seed);

        // For entity match
        public static final EnumSet<Predefined> entitySupportedSelections = EnumSet.of(ID, Seed, LeadToAcct);

        private static Map<String, Predefined> nameMap;

        static {
            nameMap = new HashMap<>();
            for (Predefined predefined : Predefined.values()) {
                nameMap.put(predefined.getName(), predefined);
            }
        }

        private final String name;

        Predefined(String name) {
            this.name = name;
        }

        public static Predefined fromName(String name) {
            return nameMap.get(name);
        }

        public static Predefined getLegacyDefaultSelection() {
            return DerivedColumns;
        }

        public static Predefined getDefaultSelection() {
            return RTS;
        }

        public static List<String> getNames() {
            return new ArrayList<>(nameMap.keySet());
        }

        @MetricTag(tag = "PredefinedSelection")
        public String getName() {
            return this.name;
        }

        public String getJsonFileName(String version) {
            return getName() + "_" + version + ".json";
        }

    }
}
