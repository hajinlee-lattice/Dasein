package com.latticeengines.domain.exposed.redshift;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RedshiftTableConfiguration {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("sort_key_type")
    private SortKeyType sortKeyType;

    @JsonProperty("sort_keys")
    private List<String> sortKeys;

    @JsonProperty("dist_style")
    private DistKeyStyle distStyle;

    @JsonProperty("dist_key")
    private String distKey;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setSortKeys(List<String> sortKeys) {
        this.sortKeys = sortKeys;
    }

    public DistKeyStyle getDistStyle() {
        return distStyle;
    }

    public void setDistStyle(DistKeyStyle distStyle) {
        this.distStyle = distStyle;
    }

    public SortKeyType getSortKeyType() {
        return sortKeyType;
    }

    public void setSortKeyType(SortKeyType sortKeyType) {
        this.sortKeyType = sortKeyType;
    }

    public String getDistKey() {
        return distKey;
    }

    public void setDistKey(String distKey) {
        this.distKey = distKey;
    }

    public enum SortKeyType {
        Compound("compound"), //
        Interleaved("interleaved");

        private String name;

        SortKeyType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }

    public enum DistKeyStyle {
        All("all"), //
        Even("even"), //
        Key("key");

        private String name;

        DistKeyStyle(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }

}
