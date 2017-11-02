package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnMetadata implements HasAttributeCustomizations {
    public static final String SUBCATEGORY_OTHER = "Other";

    @JsonProperty("ColumnId")
    private String columnId;
    @JsonProperty("ColumnName")
    private String columnName;
    @JsonProperty("Description")
    private String description;
    @JsonProperty("DataType")
    private String dataType;
    @JsonProperty("JavaClass")
    private String javaClass;
    @JsonProperty("DisplayName")
    private String displayName;
    private Category category;
    @JsonProperty("Subcategory")
    private String subcategory;
    @JsonProperty("DiscretizationStrategy")
    private String discretizationStrategy;
    @JsonProperty("MatchDestination")
    private String matchDestination;
    @JsonProperty("Entity")
    private BusinessEntity entity;

    private StatisticalType statisticalType;
    private FundamentalType fundamentalType;
    private String approvedUsage;
    private String tags;
    private String decodeStrategy;
    private LogicalDataType logicalDataType;

    @JsonProperty("Groups")
    private List<ColumnSelection.Predefined> groups;

    @JsonProperty("BitOffset")
    private Integer bitOffset;
    @JsonProperty("NumBits")
    private Integer numBits;
    @JsonProperty("PhysicalName")
    private String physicalName;
    @JsonProperty("Stats")
    private AttributeStats stats;

    @JsonProperty("IsPremium")
    private Boolean isPremium;
    @JsonProperty("CanModel")
    private Boolean canModel;
    @JsonProperty("CanEnrich")
    private Boolean canEnrich;
    @JsonProperty("CanInternalEnrich")
    private Boolean canInternalEnrich;
    @JsonProperty("CanInsights")
    private Boolean canInsights;
    @JsonProperty("CanBIS")
    private Boolean canBis;

    @JsonProperty("AttributeFlagsMap")
    private Map<AttributeUseCase, JsonNode> attributeFlagsMap;

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    @JsonIgnore
    public String getName() {
        return getColumnId();
    }

    @Deprecated
    public String getColumnName() {
        return columnName;
    }

    @Deprecated
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Deprecated
    public String getDataType() {
        return dataType;
    }

    @Deprecated
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    public String getDisplayName() {
        return displayName;
    }

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

    public String getDiscretizationStrategy() {
        return discretizationStrategy;
    }

    public void setDiscretizationStrategy(String discretizationStrategy) {
        this.discretizationStrategy = discretizationStrategy;
    }

    public String getMatchDestination() {
        return matchDestination;
    }

    public void setMatchDestination(String matchDestination) {
        this.matchDestination = matchDestination;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    @JsonIgnore
    public Boolean isPremium() {
        return isPremium;
    }

    @JsonIgnore
    public void setIsPremium(Boolean isPremium) {
        this.isPremium = isPremium;
    }

    public Boolean isCanModel() {
        return canModel;
    }

    public void setCanModel(Boolean canModel) {
        this.canModel = canModel;
    }

    public Boolean isCanEnrich() {
        return canEnrich;
    }

    public void setCanEnrich(Boolean canEnrich) {
        this.canEnrich = canEnrich;
    }

    public Boolean isCanInternalEnrich() {
        return canInternalEnrich;
    }

    public void setCanInternalEnrich(Boolean canInternalEnrich) {
        this.canInternalEnrich = canInternalEnrich;
    }

    public Boolean isCanInsights() {
        return canInsights;
    }

    public void setCanInsights(Boolean canInsights) {
        this.canInsights = canInsights;
    }

    public Boolean isCanBis() {
        return canBis;
    }

    public void setCanBis(Boolean canBis) {
        this.canBis = canBis;
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

    public void removeApprovedUsageList() {
        this.approvedUsage = null;
    }

    public List<ColumnSelection.Predefined> getGroups() {
        return groups;
    }

    public void setGroups(List<ColumnSelection.Predefined> groups) {
        this.groups = groups;
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

    public String getSubcategory() {
        return subcategory;
    }

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

    public void removeTagList() {
        this.tags = null;
    }

    public String getDecodeStrategy() {
        return decodeStrategy;
    }

    public void setDecodeStrategy(String decodeStrategy) {
        this.decodeStrategy = decodeStrategy;
    }

    @Override
    public Map<AttributeUseCase, JsonNode> getAttributeFlagsMap() {
        return attributeFlagsMap;
    }

    @Override
    public void setAttributeFlagsMap(Map<AttributeUseCase, JsonNode> attributeFlagsMap) {
        this.attributeFlagsMap = attributeFlagsMap;
    }

    public Integer getBitOffset() {
        return bitOffset;
    }

    public void setBitOffset(Integer bitOffset) {
        this.bitOffset = bitOffset;
    }

    public Integer getNumBits() {
        return numBits;
    }

    public void setNumBits(Integer numBits) {
        this.numBits = numBits;
    }

    public String getPhysicalName() {
        return physicalName;
    }

    public void setPhysicalName(String physicalName) {
        this.physicalName = physicalName;
    }

    public AttributeStats getStats() {
        return stats;
    }

    public void setStats(AttributeStats stats) {
        this.stats = stats;
    }

    public LogicalDataType getLogicalDataType() {
        return logicalDataType;
    }

    public void setLogicalDataType(LogicalDataType logicalDataType) {
        this.logicalDataType = logicalDataType;
    }
}
