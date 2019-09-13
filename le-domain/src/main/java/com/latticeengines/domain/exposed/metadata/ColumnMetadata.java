package com.latticeengines.domain.exposed.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class ColumnMetadata implements HasAttributeCustomizations, Serializable {
    private static final long serialVersionUID = -8532367815438372761L;

    @JsonProperty(ColumnMetadataKey.AttrName)
    private String attrName;
    @JsonProperty(ColumnMetadataKey.Description)
    private String description;
    @JsonProperty(ColumnMetadataKey.DisplayName)
    private String displayName;
    @JsonProperty(ColumnMetadataKey.SecondaryDisplayName)
    private String secondaryDisplayName;
    private Category category;
    @JsonProperty(ColumnMetadataKey.Subcategory)
    private String subcategory;
    @JsonProperty(ColumnMetadataKey.Entity)
    private BusinessEntity entity;
    @JsonProperty(ColumnMetadataKey.State)
    private AttrState attrState;

    private StatisticalType statisticalType;
    private FundamentalType fundamentalType;
    private String approvedUsage;
    private String tags;
    private String decodeStrategy;

    @JsonProperty("DataType")
    private String dataType;
    @JsonProperty("JavaClass")
    private String javaClass;
    @JsonProperty("ImportanceOrdering")
    private Integer importanceOrdering;
    @JsonProperty("LogicalDataType")
    private LogicalDataType logicalDataType;
    @JsonProperty("DiscretizationStrategy")
    private String discretizationStrategy;
    @JsonProperty("Groups")
    private Map<ColumnSelection.Predefined, Boolean> groups;

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
    @JsonProperty("CanEnrich")
    private Boolean canEnrich;
    @JsonProperty("CanSegment")
    private Boolean canSegment;
    @JsonProperty("CanModel")
    private Boolean canModel;
    @JsonProperty("CanInternalEnrich")
    private Boolean canInternalEnrich;
    @JsonProperty("ShouldDeprecate")
    private Boolean shouldDeprecate;
    @JsonProperty("DataLicense")
    private String dataLicense;
    @JsonProperty("IsHiddenForRemodelingUI")
    private boolean isHiddenForRemodelingUI;

    @JsonProperty("IsCoveredByOptionalRule")
    private boolean isCoveredByOptionalRule;

    @JsonProperty("IsCoveredByMandatoryRule")
    private boolean isCoveredByMandatoryRule;

    @JsonProperty("AssociatedDataRules")
    private List<String> AssociatedDataRules;

    @JsonProperty("PredictivePower")
    private Double predictivePower;

    @JsonProperty("LastDataRefresh")
    private String lastDataRefresh;

    @JsonProperty("IsCampaignDerivedField")
    private Boolean isCampaignDerivedField = false;

    @JsonProperty("RefreshFrequency")
    private RefreshFrequency refreshFrequency;

    // TODO: Attribute Customization should be migrated to new metadata
    // framework
    @Deprecated
    @JsonProperty("AttributeFlagsMap")
    private Map<AttributeUseCase, JsonNode> attributeFlagsMap;

    @Deprecated
    @JsonProperty("MatchDestination")
    private String matchDestination;

    @Deprecated
    @JsonProperty("ColumnName")
    private String columnName;

    public ColumnMetadata() {

    }

    public ColumnMetadata(String attrName, String javaClass) {
        this.attrName = attrName;
        this.javaClass = javaClass;
    }

    @Override
    @Deprecated // should use AttrName
    @JsonProperty("ColumnId")
    public String getColumnId() {
        return getAttrName();
    }

    @Deprecated
    @JsonProperty("ColumnId")
    public void setColumnId(String columnId) {
        setAttrName(columnId);
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
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

    public AttrState getAttrState() {
        return attrState;
    }

    public void setAttrState(AttrState attrState) {
        this.attrState = attrState;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getSecondaryDisplayName() {
        return secondaryDisplayName;
    }

    public void setSecondaryDisplayName(String secondaryDisplayName) {
        this.secondaryDisplayName = secondaryDisplayName;
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

    @Deprecated
    public String getMatchDestination() {
        return matchDestination;
    }

    @Deprecated
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

    public Boolean getCanInternalEnrich() {
        return canInternalEnrich;
    }

    public void setCanInternalEnrich(Boolean canInternalEnrich) {
        this.canInternalEnrich = canInternalEnrich;
    }

    public Boolean getCanEnrich() {
        return canEnrich;
    }

    public void setCanEnrich(Boolean canExternalEnrich) {
        this.canEnrich = canExternalEnrich;
    }

    public Boolean getCanSegment() {
        return canSegment;
    }

    public void setCanSegment(Boolean canSegment) {
        this.canSegment = canSegment;
    }

    public Boolean getCanModel() {
        return canModel;
    }

    public void setCanModel(Boolean canModel) {
        this.canModel = canModel;
    }

    public Boolean getShouldDeprecate() {
        return shouldDeprecate;
    }

    public void setShouldDeprecate(Boolean shouldDeprecate) {
        this.shouldDeprecate = shouldDeprecate;
    }

    public String getDataLicense() {
        return dataLicense;
    }

    public void setDataLicense(String dataLicense) {
        this.dataLicense = dataLicense;
    }

    public boolean isHiddenForRemodelingUI() {
        return isHiddenForRemodelingUI;
    }

    public void setIsHiddenForRemodelingUI(boolean isHiddenForRemodelingUI) {
        this.isHiddenForRemodelingUI = isHiddenForRemodelingUI;
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
            return null;
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

    @Override
    @JsonProperty(ColumnMetadataKey.Category)
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

    public Map<ColumnSelection.Predefined, Boolean> getGroups() {
        return groups;
    }

    public void setGroups(Map<ColumnSelection.Predefined, Boolean> groups) {
        if (MapUtils.isNotEmpty(groups)) {
            this.groups = new HashMap<>();
            groups.forEach((k, v) -> {
                if (k != null && v != null) {
                    this.groups.put(k, v);
                }
            });
            if (MapUtils.isEmpty(groups)) {
                this.groups = null;
            }
        } else {
            this.groups = null;
        }
    }

    public void enableGroupIfNotPresent(ColumnSelection.Predefined group) {
        if (MapUtils.isEmpty(groups) || !groups.containsKey(group)) {
            enableGroup(group);
        }
    }

    public void disableGroupIfNotPresent(ColumnSelection.Predefined group) {
        if (MapUtils.isEmpty(groups) || !groups.containsKey(group)) {
            disableGroup(group);
        }
    }

    public void enableGroup(ColumnSelection.Predefined group) {
        if (groups == null) {
            groups = new HashMap<>();
        }
        groups.put(group, true);
    }

    public void disableGroup(ColumnSelection.Predefined group) {
        if (groups == null) {
            groups = new HashMap<>();
        }
        groups.put(group, false);
    }

    public void unsetGroup(ColumnSelection.Predefined group) {
        if (MapUtils.isNotEmpty(groups) && groups.containsKey(group)) {
            groups.remove(group);
        }
    }

    public boolean isEnabledFor(ColumnSelection.Predefined group) {
        return MapUtils.isNotEmpty(groups) && groups.getOrDefault(group, false);
    }

    // for backward compatible to list-format groups
    @Deprecated
    public List<ColumnSelection.Predefined> getEnabledGroups() {
        if (MapUtils.isNotEmpty(groups)) {
            return groups.entrySet().stream() //
                    .filter(e -> Boolean.TRUE.equals(e.getValue())) //
                    .map(Map.Entry::getKey) //
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    @JsonProperty(ColumnMetadataKey.Category)
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

    @Override
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
            return null;
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

    public Integer getImportanceOrdering() {
        return importanceOrdering;
    }

    public void setImportanceOrdering(Integer importanceOrdering) {
        this.importanceOrdering = importanceOrdering;
    }

    public boolean isCoveredByOptionalRule() {
        return isCoveredByOptionalRule;
    }

    public void setIsCoveredByOptionalRule(boolean isCoveredByOptionalRule) {
        this.isCoveredByOptionalRule = isCoveredByOptionalRule;
    }

    public boolean isCoveredByMandatoryRule() {
        return isCoveredByMandatoryRule;
    }

    public void setIsCoveredByMandatoryRule(boolean isCoveredByMandatoryRule) {
        this.isCoveredByMandatoryRule = isCoveredByMandatoryRule;
    }

    public List<String> getAssociatedDataRules() {
        return AssociatedDataRules;
    }

    public void setAssociatedDataRules(List<String> associatedDataRules) {
        AssociatedDataRules = associatedDataRules;
    }

    public Double getPredictivePower() {
        return predictivePower;
    }

    public void setPredictivePower(Double predictivePower) {
        this.predictivePower = predictivePower;
    }

    public String getLastDataRefresh() {
        return lastDataRefresh;
    }

    public void setLastDataRefresh(String lastDataRefresh) {
        this.lastDataRefresh = lastDataRefresh;
    }

    public Boolean isCampaignDerivedField() {
        return isCampaignDerivedField;
    }

    public void setIsCampaignDerivedField(Boolean isCampaignDerivedField) {
        this.isCampaignDerivedField = isCampaignDerivedField;
    }

    public boolean isDateAttribute() {
        return FundamentalType.DATE.equals(this.getFundamentalType())
                || LogicalDataType.Date.equals(this.getLogicalDataType())
                || LogicalDataType.Timestamp.equals(this.getLogicalDataType());
    }

    public RefreshFrequency getRefreshFrequency() {
        return refreshFrequency;
    }

    public void setRefreshFrequency(RefreshFrequency refreshFrequency) {
        this.refreshFrequency = refreshFrequency;
    }

    @Override
    public ColumnMetadata clone() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, ColumnMetadata.class);
    }
}
