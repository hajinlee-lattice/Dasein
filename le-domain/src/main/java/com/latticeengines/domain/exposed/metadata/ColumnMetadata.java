package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMetadata {


    private String columnId;
    private String columnName;
    private String description;
    private String dataType;
    private String displayName;
    private Category category;
    private StatisticalType statisticalType;
    private FundamentalType fundamentalType;
    private String approvedUsage;
    private String tags;
    private String discretizationStrategy;
    private String matchDestination;
    private Boolean isPremium = Boolean.FALSE;

    @JsonProperty("ColumnId")
    public String getColumnId() {
        return columnId;
    }

    @JsonProperty("ColumnId")
    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    @JsonProperty("ColumnName")
    public String getColumnName() {
        return columnName;
    }

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
    private String getTags() {
        return tags;
    }

    @JsonIgnore
    private void setTags(String tags) {
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

    @JsonProperty("IsPremium")
    public Boolean isPremium() {
        return isPremium;
    }

    @JsonProperty("IsPremium")
    public void setIsPremium(Boolean isPremium) {
        this.isPremium = isPremium;
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
        if (StringUtils.isEmpty(approvedUsage)) {
            return approvedUsages;
        }
        List<String> tokens = Arrays.asList(approvedUsage.split(","));
        for (String token : tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        return approvedUsages;
    }

    @JsonIgnore
    public void setApprovedUsageList(List<ApprovedUsage> approvedUsages) {
        List<String> tokens = new ArrayList<>();
        if (approvedUsages == null) { approvedUsages = new ArrayList<>(); }
        for (ApprovedUsage usage : approvedUsages) {
            tokens.add(usage.getName());
        }
        this.approvedUsage = StringUtils.join(tokens, ",");
    }

    @JsonProperty("ApprovedUsage")
    private List<String> getApprovedUsageJson() {
        List<String> tokens = new ArrayList<>();
        List<ApprovedUsage> approvedUsages = getApprovedUsageList();
        if (approvedUsages.isEmpty()) {
            return tokens;
        }
        for (ApprovedUsage approvedUsage : approvedUsages) {
            tokens.add(approvedUsage.getName());
        }
        return tokens;
    }

    @JsonProperty("ApprovedUsage")
    private void setApprovedUsageJson(List<String> tokens) {
        List<ApprovedUsage> approvedUsages = new ArrayList<>();
        for (String token : tokens) {
            approvedUsages.add(ApprovedUsage.fromName(token));
        }
        setApprovedUsageList(approvedUsages);
    }

    @JsonIgnore
    public String getApprovedUsageString() {
        List<String> tokens = getApprovedUsageJson();
        return "[" + StringUtils.join(tokens, ",") + "]";
    }

    @JsonProperty("Category")
    public String getCategoryAsString() {
        if (category != null) {
            return category.getName();
        } else {
            return null;
        }
    }

    @JsonProperty("Category")
    public void setCategoryByString(String categoryName) {
        setCategory(Category.fromName(categoryName));
    }

    @JsonIgnore
    public Category getCategory() {
        return category;
    }

    @JsonIgnore
    public void setCategory(Category category) {
        this.category = category;
    }

    @JsonProperty("Tags")
    public List<Tag> getTagList() {
        List<Tag> tags = new ArrayList<>();
        if (StringUtils.isEmpty(this.tags)) {
            return tags;
        }
        for (String tagName: Arrays.asList(this.tags.split(","))) {
            if (Tag.availableNames().contains(tagName)) {
                tags.add(Tag.fromName(tagName));
            }
        }
        return tags;
    }

    @JsonProperty("Tags")
    public void setTagList(List<Tag> tags) {
        List<String> tokens = new ArrayList<>();
        if (tags == null) { tags = new ArrayList<>(); }
        for (Tag tag : tags) {
            tokens.add(tag.getName());
        }
        this.tags = StringUtils.join(tokens, ",");
    }

}
