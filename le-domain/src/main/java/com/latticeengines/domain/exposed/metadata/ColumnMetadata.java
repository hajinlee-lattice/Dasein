package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMetadata implements HasAttributeCustomizations {
    public static final String SUBCATEGORY_OTHER = "Other";

    private String columnId;
    private String columnName;
    private String description;
    private String dataType;
    private String javaClass;
    private String displayName;
    private Category category;
    private String subcategory;
    private StatisticalType statisticalType;
    private FundamentalType fundamentalType;
    private String approvedUsage;
    private String tags;
    private String discretizationStrategy;
    private String matchDestination;
    private boolean isPremium;
    private String decodeStrategy;

    @JsonProperty("ObjectType")
    private SchemaInterpretation objectType;

    @JsonProperty("BucketForSegment")
    private boolean bucketForSegment;

    @JsonProperty("CanModel")
    private boolean canModel;

    @JsonProperty("CanEnrich")
    private boolean canEnrich;

    @JsonProperty("CanInternalEnrich")
    private boolean canInternalEnrich;

    @JsonProperty("CanInsights")
    private boolean canInsights;

    @JsonProperty("CanBIS")
    private boolean canBis;

    @JsonProperty("AttributeFlagsMap")
    private Map<AttributeUseCase, JsonNode> attributeFlagsMap;

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

    @JsonProperty("JavaClass")
    public String getJavaClass() {
        return javaClass;
    }

    @JsonProperty("JavaClass")
    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
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
    public boolean isPremium() {
        return isPremium;
    }

    @JsonProperty("IsPremium")
    public void setIsPremium(boolean isPremium) {
        this.isPremium = isPremium;
    }

    public boolean isCanModel() {
        return canModel;
    }

    public void setCanModel(boolean canModel) {
        this.canModel = canModel;
    }

    public boolean isCanEnrich() {
        return canEnrich;
    }

    public void setCanEnrich(boolean canEnrich) {
        this.canEnrich = canEnrich;
    }

    public boolean isCanInternalEnrich() {
        return canInternalEnrich;
    }

    public void setCanInternalEnrich(boolean canInternalEnrich) {
        this.canInternalEnrich = canInternalEnrich;
    }

    public boolean isCanInsights() {
        return canInsights;
    }

    public void setCanInsights(boolean canInsights) {
        this.canInsights = canInsights;
    }

    public boolean isCanBis() {
        return canBis;
    }

    public void setCanBis(boolean canBis) {
        this.canBis = canBis;
    }

    public boolean isBucketForSegment() {
        return bucketForSegment;
    }

    public void setBucketForSegment(boolean bucketForSegment) {
        this.bucketForSegment = bucketForSegment;
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
        if (fundamentalType != null) {
            setFundamentalType(FundamentalType.fromName(fundamentalType));
        } else {
            setFundamentalType(null);
        }
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
        if (approvedUsages == null) {
            approvedUsages = new ArrayList<>();
        }
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

    @JsonProperty("Subcategory")
    public String getSubcategory() {
        return subcategory;
    }

    @JsonProperty("Subcategory")
    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    @JsonProperty("Tags")
    public List<Tag> getTagList() {
        List<Tag> tags = new ArrayList<>();
        if (StringUtils.isEmpty(this.tags)) {
            return tags;
        }
        for (String tagName : Arrays.asList(this.tags.split(","))) {
            if (Tag.availableNames().contains(tagName)) {
                tags.add(Tag.fromName(tagName));
            }
        }
        return tags;
    }

    @JsonProperty("Tags")
    public void setTagList(List<Tag> tags) {
        List<String> tokens = new ArrayList<>();
        if (tags == null) {
            tags = new ArrayList<>();
        }
        for (Tag tag : tags) {
            tokens.add(tag.getName());
        }
        this.tags = StringUtils.join(tokens, ",");
    }

    public String getDecodeStrategy() {
        return decodeStrategy;
    }

    public void setDecodeStrategy(String decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
    }

    public SchemaInterpretation getObjectType() {
        return objectType;
    }

    public void setObjectType(SchemaInterpretation objectType) {
        this.objectType = objectType;
    }

    @Override
    public Map<AttributeUseCase, JsonNode> getAttributeFlagsMap() {
        return attributeFlagsMap;
    }

    @Override
    public void setAttributeFlagsMap(Map<AttributeUseCase, JsonNode> attributeFlagsMap) {
        this.attributeFlagsMap = attributeFlagsMap;
    }

}
