package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class VdbMetadataField {

    private String columnName;
    private String source; // source type
    private String sourceToDisplay;
    private String object; // table name
    private String category;
    private String displayName;
    private String approvedUsage;
    private String tags;
    private String fundamentalType;
    private String description;
    private String displayDiscretization;
    private String statisticalType;

    @JsonProperty("ColumnName")
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty("ColumnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty("Source")
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @JsonProperty("SourceToDisplay")
    public String getSourceToDisplay() {
        return sourceToDisplay;
    }

    @JsonProperty("SourceToDisplay")
    public void setSourceToDisplay(String sourceToDisplay) {
        this.sourceToDisplay = sourceToDisplay;
    }

    @JsonProperty("Object")
    public String getObject() {
        return object;
    }

    @JsonProperty("Object")
    public void setObject(String object) {
        this.object = object;
    }

    @JsonProperty("Category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("Category")
    public void setCategory(String category) {
        this.category = category;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("ApprovedUsage")
    public String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonProperty("ApprovedUsage")
    public void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    @JsonProperty("Tags")
    public String getTags() {
        return tags;
    }

    @JsonProperty("Tags")
    public void setTags(String tags) {
        this.tags = tags;
    }

    @JsonProperty("FundamentalType")
    public String getFundamentalType() {
        return fundamentalType;
    }

    @JsonProperty("FundamentalType")
    public void setFundamentalType(String fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("DisplayDiscretization")
    public String getDisplayDiscretization() {
        return displayDiscretization;
    }

    @JsonProperty("DisplayDiscretization")
    public void setDisplayDiscretization(String displayDiscretization) {
        this.displayDiscretization = displayDiscretization;
    }

    @JsonProperty("StatisticalType")
    public String getStatisticalType() {
        return statisticalType;
    }

    @JsonProperty("StatisticalType")
    public void setStatisticalType(String statisticalType) {
        this.statisticalType = statisticalType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
