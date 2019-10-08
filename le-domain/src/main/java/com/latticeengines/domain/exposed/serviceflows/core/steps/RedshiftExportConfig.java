package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RedshiftExportConfig {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("input_path")
    private String inputPath;

    @JsonProperty("dist_key")
    private String distKey;

    @JsonProperty("dist_style")
    private RedshiftTableConfiguration.DistStyle distStyle;

    @JsonProperty("sort_keys")
    private List<String> sortKeys;

    @JsonProperty("update_mode")
    private boolean updateMode;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getDistKey() {
        return distKey;
    }

    public void setDistKey(String distKey) {
        this.distKey = distKey;
    }

    public RedshiftTableConfiguration.DistStyle getDistStyle() {
        return distStyle;
    }

    public void setDistStyle(RedshiftTableConfiguration.DistStyle distStyle) {
        this.distStyle = distStyle;
    }

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setSortKeys(List<String> sortKeys) {
        this.sortKeys = new ArrayList<>(sortKeys);
    }

    public void setSingleSortKey(String sortKey) {
        this.sortKeys = new ArrayList<>();
        sortKeys.add(sortKey);
    }

    public boolean isUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(boolean updateMode) {
        this.updateMode = updateMode;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
