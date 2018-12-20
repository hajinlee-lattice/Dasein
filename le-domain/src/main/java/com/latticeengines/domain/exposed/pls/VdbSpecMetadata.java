package com.latticeengines.domain.exposed.pls;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbSpecMetadata {

    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("data_type")
    private String dataType;

    @JsonProperty("key_column")
    private boolean keyColumn;

    @JsonProperty("description")
    private String description;

    @JsonProperty("data_source")
    private List<String> dataSource;

    @JsonProperty("approved_usage")
    private List<String> approvedUsage;

    @JsonProperty("data_quality")
    private List<String> dataQuality;

    @JsonProperty("fundamental_type")
    private String fundamentalType;

    @JsonProperty("most_recent_update_date")
    private String mostRecentUpdateDate;

    @JsonProperty("last_time_source_updated")
    private List<String> lastTimeSourceUpdated;

    @JsonProperty("statistical_type")
    private String statisticalType;

    @JsonProperty("tags")
    private List<String> tags;

    @JsonProperty("display_discretization_strategy")
    private String displayDiscretizationStrategy;

    @JsonProperty("extensions")
    private List<VdbMetadataExtension> extensions;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(boolean keyColumn) {
        this.keyColumn = keyColumn;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getDataSource() {
        return dataSource;
    }

    public void setDataSource(List<String> dataSource) {
        this.dataSource = dataSource;
    }

    public List<String> getApprovedUsage() {
        return approvedUsage;
    }

    public void setApprovedUsage(List<String> approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    public List<String> getDataQuality() {
        return dataQuality;
    }

    public void setDataQuality(List<String> dataQuality) {
        this.dataQuality = dataQuality;
    }

    public String getFundamentalType() {
        return fundamentalType;
    }

    public void setFundamentalType(String fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    public String getMostRecentUpdateDate() {
        return mostRecentUpdateDate;
    }

    public void setMostRecentUpdateDate(String mostRecentUpdateDate) {
        this.mostRecentUpdateDate = mostRecentUpdateDate;
    }

    public List<String> getLastTimeSourceUpdated() {
        return lastTimeSourceUpdated;
    }

    public void setLastTimeSourceUpdated(List<String> lastTimeSourceUpdated) {
        this.lastTimeSourceUpdated = lastTimeSourceUpdated;
    }

    public String getStatisticalType() {
        return statisticalType;
    }

    public void setStatisticalType(String statisticalType) {
        this.statisticalType = statisticalType;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getDisplayDiscretizationStrategy() {
        return displayDiscretizationStrategy;
    }

    public void setDisplayDiscretizationStrategy(String displayDiscretizationStrategy) {
        this.displayDiscretizationStrategy = displayDiscretizationStrategy;
    }

    public List<VdbMetadataExtension> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<VdbMetadataExtension> extensions) {
        this.extensions = extensions;
    }

    @JsonIgnore
    private final static Set<String> NUMERIC_TYPE = new HashSet<>(Arrays.asList(
            "byte", "int", "long", "float", "double"));

    @JsonIgnore
    private final static Set<String> BOOLEAN_TYPE = new HashSet<>(Collections.singletonList("bit"));

    @JsonIgnore
    private final static Set<String> DATE_TYPE = new HashSet<>(Arrays.asList("date", "datetime", "datetimeoffset"));

    @JsonIgnore
    public boolean isNumericType() {
        if (this.dataType == null) {
            return false;
        }
        return NUMERIC_TYPE.contains(this.dataType.toLowerCase());
    }

    @JsonIgnore
    public boolean isDateType() {
        if (this.dataType == null) {
            return false;
        }
        return DATE_TYPE.contains(this.dataType.toLowerCase());
    }

    @JsonIgnore
    public boolean isBooleanType() {
        if (this.dataType == null) {
            return false;
        }
        return BOOLEAN_TYPE.contains(this.dataType.toLowerCase());
    }

}
