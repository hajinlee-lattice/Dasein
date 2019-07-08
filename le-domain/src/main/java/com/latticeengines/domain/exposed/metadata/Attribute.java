package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasProperty;
import com.latticeengines.domain.exposed.metadata.annotation.AttributePropertyBag;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.AttributeUtils;

@Entity
@javax.persistence.Table(name = "METADATA_ATTRIBUTE")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class Attribute
        implements HasName, HasPid, HasProperty, HasTenantId, Serializable, GraphNode {

    private static final long serialVersionUID = -4779448415471374224L;

    /*
     * For batching to work, we need to make a schema change to take off
     * AutoIncrement from ID column. Because of code difference between
     * ActiveStack and InActiveStack we can't apply the Schema changes in
     * advance. To introduce batching at this time by considering activities
     * from both Active and InActive stacks, we need to consider addition of new
     * column with backward compatibility support. As it is unnecessary at this
     * point, going back to Identity generator.
     *
     * public static final int MDATTRIBUTE_INIT_VALUE = 300_000_000; // 300M
     *
     * @TableGenerator(name = "MDAttribute_SEQ_GEN", table =
     * "PLS_MULTITENANT_SEQ_ID", pkColumnName = "SEQUENCE_NAME", valueColumnName
     * = "SEQUENCE_VAL", pkColumnValue = "MDAttribute_SEQUENCE", initialValue =
     * MDATTRIBUTE_INIT_VALUE, allocationSize = 50)
     *
     * @GeneratedValue(strategy = GenerationType.TABLE, generator =
     * "MDAttribute_SEQ_GEN")
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("display_name")
    private String displayName;

    @Column(name = "SECONDARY_DISPLAY_NAME", length = 1000)
    @JsonProperty("secondary_display_name")
    private String secondaryDisplayName;

    @Column(name = "LENGTH")
    @JsonProperty("length")
    private Integer length;

    @Column(name = "NULLABLE", nullable = false)
    @JsonProperty("nullable")
    private Boolean nullable = Boolean.FALSE;

    @Column(name = "DATA_TYPE", nullable = false)
    @JsonProperty("physical_data_type")
    private String physicalDataType;

    @Column(name = "SOURCE_LOGICAL_DATA_TYPE")
    @JsonProperty("source_logical_type")
    private String sourceLogicalDataType;

    @Column(name = "LOGICAL_DATA_TYPE")
    @JsonProperty("logical_type")
    @Enumerated(EnumType.STRING)
    private LogicalDataType logicalDataType;

    @Column(name = "PRECISION")
    @JsonProperty("precision")
    private Integer precision;

    @Column(name = "SCALE")
    @JsonProperty("scale")
    private Integer scale;

    @Transient
    @JsonIgnore
    private List<String> cleanedUpEnumValues = new ArrayList<>();

    @Column(name = "ENUM_VALUES", length = 2048)
    @JsonProperty("enum_values")
    private String cleanedUpEnumValuesAsString = "";

    @Column(name = "PROPERTIES", nullable = false)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    @AttributePropertyBag
    @JsonProperty("properties")
    private Map<String, Object> properties = new HashMap<>();

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    private Table table;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    @Column(name = "VALIDATORS")
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    @JsonProperty("validatorWrappers")
    private List<InputValidatorWrapper> validatorWrappers = new ArrayList<>();

    @Column(name = "GROUPS", length = 1000)
    @JsonIgnore
    private String groups;

    public Attribute() {
    }

    public Attribute(String name) {
        this.name = name;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
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

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * Corresponds to an avro data type. Only possible values are in
     * Schema.Type.
     */

    public String getPhysicalDataType() {
        return physicalDataType;
    }

    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public LogicalDataType getLogicalDataType() {
        return logicalDataType;
    }

    public void setLogicalDataType(String logicalDataTypeString) {
        LogicalDataType logicalDataType = null;
        try {
            logicalDataType = LogicalDataType.valueOf(logicalDataTypeString);
        } catch (Exception e) {
            // pass
        }
        if (logicalDataType != null) {
            setLogicalDataType(logicalDataType);
        }
    }

    public void setLogicalDataType(LogicalDataType logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    /**
     * The logical data type of the source. For example, in the case of SFDC
     * which has a logical data type called PhoneNumber, this would be set to
     * PhoneNumber.
     */
    public String getSourceLogicalDataType() {
        return sourceLogicalDataType;
    }

    @JsonProperty("source_logical_type")
    public void setSourceLogicalDataType(String sourceLogicalDataType) {
        this.sourceLogicalDataType = sourceLogicalDataType;
    }

    public InterfaceName getInterfaceName() {
        Object raw = properties.get("InterfaceName");
        if (raw == null) {
            return null;
        }
        try {
            return InterfaceName.valueOf(raw.toString());
        } catch (Exception e) {
            return null;
        }
    }

    public void setInterfaceName(String interfaceNameString) {
        InterfaceName interfaceName = null;
        try {
            interfaceName = InterfaceName.valueOf(interfaceNameString);
        } catch (Exception e) {
            // pass
        }
        if (interfaceName != null) {
            setInterfaceName(interfaceName);
        }
    }

    public void setInterfaceName(InterfaceName interfaceName) {
        if (interfaceName != null) {
            properties.put("InterfaceName", interfaceName.toString());
        }
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public List<String> getCleanedUpEnumValues() {
        return cleanedUpEnumValues;
    }

    public void setCleanedUpEnumValues(List<String> cleanedUpEnumValues) {
        this.cleanedUpEnumValues = cleanedUpEnumValues;
        setCleanedUpEnumValuesAsString(StringUtils.join(cleanedUpEnumValues, ","));
    }

    public String getCleanedUpEnumValuesAsString() {
        return cleanedUpEnumValuesAsString;
    }

    public void setCleanedUpEnumValuesAsString(String enumValues) {
        this.cleanedUpEnumValuesAsString = enumValues;
    }

    @Override
    public Object getPropertyValue(String key) {
        return properties.get(key);
    }

    @Override
    public void setPropertyValue(String key, Object value) {
        properties.put(key, value);
    }

    private void setListPropertyFromString(String key, String value) {
        Pattern pattern = Pattern.compile("^\\[(.*)\\]$");
        if (value != null) {
            Matcher matcher = pattern.matcher(value);
            if (matcher.matches()) {
                String contents = matcher.group(1);
                if (contents.isEmpty()) {
                    setPropertyValue(key, new ArrayList<>());
                } else {
                    String[] array = contents.split(",");
                    for (int i = 0; i < array.length; ++i) {
                        array[i] = array[i].trim();
                    }
                    setPropertyValue(key, new ArrayList<>(Arrays.asList(array)));
                }
            } else {
                setPropertyValue(key, new ArrayList<>(Arrays.asList(value)));
            }
        } else {
            setPropertyValue(key, new ArrayList<>(Arrays.asList(value)));
        }
    }

    private String getGroups() {
        return groups;
    }

    private void setGroups(String groups) {
        this.groups = groups;
    }

    @JsonProperty("groups")
    public List<ColumnSelection.Predefined> getGroupsAsList() {
        if (StringUtils.isNotBlank(groups)) {
            return Arrays.stream(groups.split(",")) //
                    .map(ColumnSelection.Predefined::fromName) //
                    .filter(Objects::nonNull) //
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    @JsonProperty("groups")
    public void setGroupsViaList(List<ColumnSelection.Predefined> groupList) {
        if (CollectionUtils.isEmpty(groupList)) {
            this.groups = null;
        } else {
            List<String> groupNames = groupList.stream() //
                    .map(ColumnSelection.Predefined::getName) //
                    .collect(Collectors.toList());
            this.groups = StringUtils.join(groupNames, ",");
        }
    }

    @Override
    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @AttributePropertyBag
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public List<InputValidatorWrapper> getValidatorWrappers() {
        return validatorWrappers;
    }

    public void setValidatorWrappers(List<InputValidatorWrapper> validatorWrappers) {
        this.validatorWrappers = validatorWrappers;
    }

    public List<InputValidator> getValidators() {
        List<InputValidator> validators = new ArrayList<>();
        for (InputValidatorWrapper raw : validatorWrappers) {
            if (raw.getValidator() != null) {
                validators.add(raw.getValidator());
            }
        }

        return validators;
    }

    public void addValidator(InputValidator validator) {
        InputValidatorWrapper wrapper = new InputValidatorWrapper(validator);
        validatorWrappers.add(wrapper);
    }

    public String getSourceAttrName() {
        return getPropertyValue("SourceAttrName") != null
                ? getPropertyValue("SourceAttrName").toString() : null;
    }

    public void setSourceAttrName(String sourceAttrName) {
        setPropertyValue("SourceAttrName", sourceAttrName);
    }

    public void setApprovedUsage(String approvedUsage) {
        setListPropertyFromString("ApprovedUsage", approvedUsage);
    }

    public void setApprovedUsage(ApprovedUsage... approvedUsages) {
        properties.put("ApprovedUsage", getStringValuesFromEnums(approvedUsages));
    }

    public void setApprovedUsageFromEnumList(List<ApprovedUsage> approvedUsage) {
        properties.put("ApprovedUsage", approvedUsage);
    }

    @SuppressWarnings("unchecked")
    public List<String> getApprovedUsage() {
        return (List<String>) properties.get("ApprovedUsage");
    }

    public void setApprovedUsage(List<String> approvedUsage) {
        properties.put("ApprovedUsage", approvedUsage);
    }

    public boolean isHiddenForRemodelingUI() {
        if (properties.containsKey("IsHiddenForRemodelingUI")) {
            return (Boolean) properties.get("IsHiddenForRemodelingUI");
        }
        return false;
    }

    public void setIsHiddenForRemodelingUI(boolean isHiddenForRemodelingUI) {
        properties.put("IsHiddenForRemodelingUI", isHiddenForRemodelingUI);
    }

    public boolean getIsCoveredByOptionalRule() {
        if (properties.containsKey("IsCoveredByOptionalRule")) {
            return (Boolean) properties.get("IsCoveredByOptionalRule");
        }
        return false;
    }

    public void setIsCoveredByOptionalRule(boolean isCoveredByOptionalRule) {
        properties.put("IsCoveredByOptionalRule", isCoveredByOptionalRule);
    }

    public boolean getIsCoveredByMandatoryRule() {
        if (properties.containsKey("IsCoveredByMandatoryRule")) {
            return (Boolean) properties.get("IsCoveredByMandatoryRule");
        }
        return false;
    }

    public void setIsCoveredByMandatoryRule(boolean isCoveredByMandatoryRule) {
        properties.put("IsCoveredByMandatoryRule", isCoveredByMandatoryRule);
    }

    @SuppressWarnings("unchecked")
    public void addAssociatedDataRuleName(String dataRuleName) {
        if (!properties.containsKey("AssociatedDataRules")) {
            properties.put("AssociatedDataRules", new ArrayList<String>());
        }
        ((List<String>) properties.get("AssociatedDataRules")).add(dataRuleName);
    }

    @SuppressWarnings("unchecked")
    public List<String> getAssociatedDataRules() {
        if (!properties.containsKey("AssociatedDataRules")) {
            return new ArrayList<>();
        }
        return (List<String>) properties.get("AssociatedDataRules");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setStatisticalType(String statisticalType) {
        if (StringUtils.isBlank(statisticalType)) {
            properties.remove("StatisticalType");
        } else {
            properties.put("StatisticalType", statisticalType);
        }
    }

    public String getStatisticalType() {
        return getPropertyValue("StatisticalType") != null
                ? getPropertyValue("StatisticalType").toString() : null;
    }

    public void setStatisticalType(StatisticalType statisticalType) {
        if (statisticalType == null) {
            properties.remove("StatisticalType");
        } else {
            properties.put("StatisticalType", statisticalType.getName());
        }
    }

    /**
     * Used for VisiDB/legacy systems
     *
     * Note(jwinter): FundamentalType is still clearly important in Atlas including its usage to distinguish
     *   Timestamp and Date type attributes from other Numeric types which also have Schema Type LONG.
     */
    public void setFundamentalType(String fundamentalType) {
        properties.put("FundamentalType", fundamentalType);
    }

    public String getFundamentalType() {
        return getPropertyValue("FundamentalType") != null
                ? getPropertyValue("FundamentalType").toString() : null;
    }

    public void setFundamentalType(FundamentalType fundamentalType) {
        if (fundamentalType == null) {
            properties.remove("FundamentalType");
        } else {
            properties.put("FundamentalType", fundamentalType.name());
        }
    }

    public String getDataQuality() {
        return (String) properties.get("DataQuality");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setDataQuality(String dataQuality) {
        properties.put("DataQuality", dataQuality);
    }

    public void setDataSource(String dataSource) {
        setListPropertyFromString("DataSource", dataSource);
    }

    @SuppressWarnings("unchecked")
    public List<String> getDataSource() {
        return (List<String>) properties.get("DataSource");
    }

    public void setDataSource(List<String> dataSource) {
        properties.put("DataSource", dataSource);
    }

    public String getDisplayDiscretizationStrategy() {
        return (String) properties.get("DisplayDiscretizationStrategy");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setDisplayDiscretizationStrategy(String displayDiscretizationStrategy) {
        properties.put("DisplayDiscretizationStrategy", displayDiscretizationStrategy);
    }

    public String getDescription() {
        return (String) properties.get("Description");
    }

    public void setDescription(String description) {
        properties.put("Description", description);
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setTags(String tags) {
        setListPropertyFromString("Tags", tags);
    }

    public void setTags(Tag... tags) {
        properties.put("Tags", getStringValuesFromEnums(tags));
    }

    @SuppressWarnings("unchecked")
    public List<String> getTags() {
        return (List<String>) properties.get("Tags");
    }

    public void setTags(List<String> tags) {
        properties.put("Tags", tags);
    }

    public boolean hasTag(Tag tag) {
        List<String> tags = getTags();
        return CollectionUtils.isNotEmpty(tags) && tags.contains(tag.toString());
    }

    public String getPhysicalName() {
        return (String) properties.get("PhysicalName");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setPhysicalName(String physicalName) {
        properties.put("PhysicalName", physicalName);
    }

    public void setCategory(String category) {
        setPropertyValue("Category", category);
    }

    public String getCategory() {
        return getPropertyValue("Category") != null ? getPropertyValue("Category").toString()
                : null;
    }

    public void setCategory(Category category) {
        setPropertyValue("Category", category.getName());
    }

    public String getSubcategory() {
        return getPropertyValue("Subcategory") != null ? getPropertyValue("Subcategory").toString()
                : null;
    }

    public void setSubcategory(String subcategory) {
        setPropertyValue("Subcategory", subcategory);
    }

    public String getDataType() {
        return getPropertyValue("DataType") != null ? getPropertyValue("DataType").toString()
                : null;
    }

    /**
     * Used for VisiDB/legacy systems
     */
    public void setDataType(String dataType) {
        setPropertyValue("DataType", dataType);
    }

    public String getRTSModuleName() {
        return (String) properties.get("RTSModuleName");
    }

    public void setRTSModuleName(String rtsModuleName) {
        properties.put("RTSModuleName", rtsModuleName);
    }

    public String getRTSArguments() {
        return (String) properties.get("RTSArguments");
    }

    public void setRTSArguments(String rtsArguments) {
        properties.put("RTSArguments", rtsArguments);
    }

    public Boolean getRTSAttribute() {
        Boolean rts = (Boolean) properties.get("RTSAttribute");
        if (rts == null) {
            return false;
        }
        return rts;
    }

    public void setRTSAttribute(String rts) {
        properties.put("RTSAttribute", Boolean.valueOf(rts));
    }

    public void setRTSAttribute(Boolean rts) {
        properties.put("RTSAttribute", rts);
    }

    public Boolean getRequired() {
        Boolean required = (Boolean) properties.get("Required");
        if (required == null) {
            return false;
        }
        return required;
    }

    public void setRequired(Boolean required) {
        properties.put("Required", required);
    }

    public String getDefaultValueStr() {
        return properties.get("DefaultValueStr") != null
                ? properties.get("DefaultValueStr").toString() : null;
    }

    public void setDefaultValueStr(String defaultValueStr) {
        properties.put("DefaultValueStr", defaultValueStr);
    }

    @SuppressWarnings("unchecked")
    public List<String> getAllowedDisplayNames() {
        return (List<String>) properties.get("AllowedDisplayNames");
    }

    public void setAllowedDisplayNames(String allowedDisplayNamesString) {
        setListPropertyFromString("AllowedDisplayNames", allowedDisplayNamesString);
    }

    public void setAllowedDisplayNames(List<String> allowedDisplayNames) {
        properties.put("AllowedDisplayNames", allowedDisplayNames);
    }

    @SuppressWarnings("unchecked")
    public List<String> getPossibleCSVNames() {
        return (List<String>) properties.get("PossibleCSVNames");
    }

    public void setPossibleCSVNames(List<String> possibleCSVNames) {
        properties.put("PossibleCSVNames", possibleCSVNames);
    }

    public void removeAllowedDisplayNames() {
        properties.remove("AllowedDisplayNames");
    }

    public Boolean getExcludeFromFiltering() {
        Boolean excludeFromFiltering = (Boolean) properties.get("ExcludeFromFiltering");
        if (excludeFromFiltering == null) {
            return false;
        }
        return excludeFromFiltering;
    }

    public void setExcludeFromFiltering(String excludeFromFiltering) {
        properties.put("ExcludeFromFiltering", Boolean.valueOf(excludeFromFiltering));
    }

    public Boolean getExcludeFromPlaymakerExport() {
        Boolean excludeFromPlaymakerExport = (Boolean) properties.get("ExcludeFromPlaymakerExport");
        if (excludeFromPlaymakerExport == null) {
            return false;
        }
        return excludeFromPlaymakerExport;
    }

    public void setExcludeFromPlaymakerExport(String excludeFromPlaymakerExport) {
        properties.put("ExcludeFromPlaymakerExport", Boolean.valueOf(excludeFromPlaymakerExport));
    }

    public Boolean getExcludeFromTalkingPoints() {
        Boolean excludeFromTalkingPoints = (Boolean) properties.get("ExcludeFromTalkingPoints");
        if (excludeFromTalkingPoints == null) {
            return false;
        }
        return excludeFromTalkingPoints;
    }

    public void setExcludeFromTalkingPoints(String excludeFromTalkingPoints) {
        properties.put("ExcludeFromTalkingPoints", Boolean.valueOf(excludeFromTalkingPoints));
    }

    public Boolean getExcludeFromListView() {
        Boolean excludeFromListView = (Boolean) properties.get("ExcludeFromListView");
        if (excludeFromListView == null) {
            return false;
        }
        return excludeFromListView;
    }

    public void setExcludeFromListView(String excludeFromListView) {
        properties.put("ExcludeFromListView", Boolean.valueOf(excludeFromListView));
    }

    public Boolean getExcludeFromDetailView() {
        Boolean excludeFromDetailView = (Boolean) properties.get("ExcludeFromDetailView");
        if (excludeFromDetailView == null) {
            return false;
        }
        return excludeFromDetailView;
    }

    public void setExcludeFromDetailView(String excludeFromDetailView) {
        properties.put("ExcludeFromDetailView", Boolean.valueOf(excludeFromDetailView));
    }

    public Boolean getExcludeFromAll() {
        Boolean excludeFromAll = (Boolean) properties.get("ExcludeFromAll");
        if (excludeFromAll == null) {
            return false;
        }
        return excludeFromAll;
    }

    public void setExcludeFromAll(String excludeFromAll) {
        properties.put("ExcludeFromAll", Boolean.valueOf(excludeFromAll));
    }

    public Integer getBitOffset() {
        return (Integer) properties.get("BitOffset");
    }

    public void setBitOffset(Integer bitOffset) {
        properties.put("BitOffset", bitOffset);
    }

    public Integer getNumOfBits() {
        return (Integer) properties.get("NumOfBits");
    }

    public void setNumOfBits(Integer numOfBits) {
        properties.put("NumOfBits", numOfBits);
    }

    @Override
    public String toString() {
        return name;
    }

    @SuppressWarnings({ "unchecked", "hiding" })
    public <Enum> List<String> getStringValuesFromEnums(Enum... enums) {
        List<String> strs = new ArrayList<>();
        for (Enum en : enums) {
            strs.add(en.toString());
        }
        return strs;
    }

    @SuppressWarnings("unchecked")
    public List<String> getParentAttributeNames() {
        List<String> parents = new ArrayList<>();

        boolean isInternalTransform = false;
        List<String> tags = getTags();
        if (tags != null) {
            for (String tag : tags) {
                if (tag.equals(Tag.INTERNAL_TRANSFORM.toString())) {
                    isInternalTransform = true;
                    break;
                }
            }
        }
        if (!isInternalTransform) {
            return parents;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, String> arguments = mapper.readValue(getRTSArguments(), Map.class);
            for (String attributeName : arguments.values()) {
                parents.add(attributeName);
            }
        } catch (Exception e) {
            return parents;
        }

        return parents;
    }

    public boolean isCustomerPredictor() {
        boolean isInternalAttributeOrTransform = false;
        List<String> tags = getTags();
        if (tags != null) {
            for (String tag : tags) {
                if (tag.equals(Tag.INTERNAL.toString())
                        || tag.equals(Tag.INTERNAL_TRANSFORM.toString())) {
                    isInternalAttributeOrTransform = true;
                    break;
                }
            }
        }
        if (!isInternalAttributeOrTransform) {
            return false;
        }
        return getInterfaceName() == null;
    }

    public boolean isInternalAndInternalTransformField() {
        List<String> tags = getTags();
        if (tags != null) {
            for (String tag : tags) {
                if (tag.equals(Tag.INTERNAL.toString())
                        || tag.equals(Tag.INTERNAL_TRANSFORM.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isInternalPredictor() {
        boolean isInternalAttribute = false;
        List<String> tags = getTags();
        if (tags != null) {
            for (String tag : tags) {
                if (tag.equals(Tag.INTERNAL.toString())) {
                    isInternalAttribute = true;
                    break;
                }
            }
        }
        return isInternalAttribute;
    }

    public ColumnMetadata getColumnMetadata() {
        return AttributeUtils.toColumnMetadata(this);
    }

    public String getDateFormatString() {
        Object raw = properties.get("DateFormatString");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    public void setDateFormatString(String dateFormatString) {
        if (dateFormatString != null) {
            properties.put("DateFormatString", dateFormatString);
        }
    }

    public String getTimeFormatString() {
        Object raw = properties.get("TimeFormatString");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    public void setTimeFormatString(String timeFormatString) {
        if (timeFormatString != null) {
            properties.put("TimeFormatString", timeFormatString);
        }
    }

    public String getTimezone() {
        Object raw = properties.get("Timezone");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    public void setTimezone(String timezone) {
        if (timezone != null) {
            properties.put("Timezone", timezone);
        }
    }

    public String getLastDataRefresh() {
        Object raw = properties.get("LastDataRefresh");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    public void setLastDataRefresh(String lastDataRefresh) {
        if (lastDataRefresh != null) {
            properties.put("LastDataRefresh", lastDataRefresh);
        }
    }
}

