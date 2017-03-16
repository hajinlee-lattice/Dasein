package com.latticeengines.domain.exposed.redshift;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class RedshiftTableConfiguration {

    @JsonProperty("table_name")
    @NotNull
    @NotEmptyString
    private String tableName;

    @JsonProperty("sort_key_type")
    private SortKeyType sortKeyType;

    @JsonProperty("sort_keys")
    private List<String> sortKeys;

    @JsonProperty("dist_style")
    private DistStyle distStyle;

    @JsonProperty("dist_key")
    private String distKey;

    @JsonProperty("json_path_prefix")
    @NotNull
    @NotEmptyString
    private String jsonPathPrefix;

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

    public DistStyle getDistStyle() {
        return distStyle;
    }

    public void setDistStyle(DistStyle distStyle) {
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

    public String getJsonPathPrefix() {
        return jsonPathPrefix;
    }

    public void setJsonPathPrefix(String jsonPathPrefix) {
        this.jsonPathPrefix = jsonPathPrefix;
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

    public enum DistStyle {
        All("all"), //
        Even("even"), //
        Key("key");

        private String name;

        DistStyle(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }

}
