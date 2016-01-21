package com.latticeengines.domain.exposed.propdata.manage;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.propdata.StatisticalType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMetadata {

    private String columnName;
    private String description;
    private String dataType;
    private String displayName;
    private String category;
    private StatisticalType statisticalType;
    private FundamentalType fundamentalType;
    private String approvedUsage;
    private String tags;
    private String discretizationStrategy;
    private String matchDestination;

    @JsonProperty("ColumnName")
    public String getColumnName() { return columnName; }

    @JsonProperty("ColumnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("DataType")
    public String getDataType() {
        return dataType;
    }

    @JsonProperty("DataType")
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("Category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("Category")
    public void setCategory(String category) {
        this.category = category;
    }

    @JsonIgnore
    public StatisticalType getStatisticalType() {
        return statisticalType;
    }

    @JsonIgnore
    public void setStatisticalType(StatisticalType statisticalType) {
        this.statisticalType = statisticalType;
    }

    @JsonIgnore
    public FundamentalType getFundamentalType() {
        return fundamentalType;
    }

    @JsonIgnore
    public void setFundamentalType(FundamentalType fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @JsonIgnore
    public String getTags() {
        return tags;
    }

    @JsonIgnore
    public void setTags(String tags) {
        this.tags = tags;
    }

    @JsonProperty("DiscretizationStrategy")
    public String getDiscretizationStrategy() {
        return discretizationStrategy;
    }

    @JsonProperty("DiscretizationStrategy")
    public void setDiscretizationStrategy(String discretizationStrategy) {
        this.discretizationStrategy = discretizationStrategy;
    }

    @JsonProperty("MatchDestination")
    public String getMatchDestination() {
        return matchDestination;
    }

    @JsonProperty("MatchDestination")
    public void setMatchDestination(String matchDestination) {
        this.matchDestination = matchDestination;
    }

    @JsonProperty("FundamentalType")
    private String getFundamentalTypeAsString() {
        if (fundamentalType == null) {
            return null;
        } else {
            return fundamentalType.getName();
        }
    }

    @JsonProperty("FundamentalType")
    private void setFundamentalTypeByString(String fundamentalType) {
        setFundamentalType(FundamentalType.fromName(fundamentalType));
    }

    @JsonProperty("StatisticalType")
    private String getStatisticalTypeAsString() {
        if (statisticalType == null) {
            return null;
        } else {
            return statisticalType.getName();
        }
    }

    @JsonProperty("StatisticalType")
    private void setStatisticalTypeByString(String statisticalType) {
        setStatisticalType(StatisticalType.fromName(statisticalType));
    }

    @JsonIgnore
    public List<ApprovedUsage> getApprovedUsageList() {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        if (StringUtils.isEmpty(approvedUsage)) {  return approvedUsages; }
        List<String> tokens = Arrays.asList(approvedUsage.split(","));
        for (String token: tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        return approvedUsages;
    }

    @JsonIgnore
    public void setApprovedUsageList(List<ApprovedUsage> approvedUsages) {
        List<String> tokens = new ArrayList<>();
        for (ApprovedUsage usage: approvedUsages) {
            tokens.add(usage.getName());
        }
        this.approvedUsage = StringUtils.join(tokens, ",");
    }

    @JsonProperty("ApprovedUsage")
    private List<String> getApprovedUsageJson() {
        List<String> tokens = new ArrayList<>();
        List<ApprovedUsage> approvedUsages = getApprovedUsageList();
        if (approvedUsages.isEmpty()) {  return tokens; }
        for (ApprovedUsage approvedUsage: approvedUsages) {
            tokens.add(approvedUsage.getName());
        }
        return tokens;
    }

    @JsonProperty("ApprovedUsage")
    private void setApprovedUsageJson(List<String> tokens) {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        for (String token: tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        setApprovedUsageList(approvedUsages);
    }

    @JsonProperty("Tags")
    public List<String> getTagList() {
        List<String> tags = new ArrayList<>();
        if (StringUtils.isEmpty(this.tags)) {  return tags; }
        tags = Arrays.asList(this.tags.split(","));
        return tags;
    }

    @JsonProperty("Tags")
    public void setTagList(List<String> tags) {
        List<String> tokens = new ArrayList<>();
        for (String tag: tags) {
            tokens.add(tag);
        }
        this.tags = StringUtils.join(tokens, ",");
    }

public ColumnMetadata() {
    	
    }

    public ColumnMetadata(ExternalColumn extenalColumn) {
    	this.setColumnName(extenalColumn.getDefaultColumnName());
    	this.setDescription(extenalColumn.getDescription());
    	this.setDataType(extenalColumn.getDataType());
    	this.setDisplayName(extenalColumn.getDisplayName());
    	this.setCategory(extenalColumn.getCategory());
		this.setStatisticalType(extenalColumn.getStatisticalType());
		this.setFundamentalType(extenalColumn.getFundamentalType());
		this.setApprovedUsageList(extenalColumn.getApprovedUsageList());
		this.setTagList(extenalColumn.getTagList());
		this.setDiscretizationStrategy(extenalColumn.getDiscretizationStrategy());
		this.setMatchDestination(extenalColumn.getColumnMappings().get(0).getSourceName());		// To be modified in ExternalColumnResource
    }
}
