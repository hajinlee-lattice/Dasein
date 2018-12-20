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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Attribute
        implements HasName, HasPid, HasProperty, HasTenantId, Serializable, GraphNode {

    public static final int MDATTRIBUTE_INIT_VALUE = 300_000_000; // 300M
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

    @Column(name = "SOURCE_LOGICAL_DATA_TYPE", nullable = true)
    @JsonProperty("source_logical_type")
    private String sourceLogicalDataType;

    @Column(name = "LOGICAL_DATA_TYPE", nullable = true)
    @JsonProperty("logical_type")
    @Enumerated(EnumType.STRING)
    private LogicalDataType logicalDataType;

    @Column(name = "PRECISION", nullable = true)
    @JsonProperty("precision")
    private Integer precision;

    @Column(name = "SCALE", nullable = true)
    @JsonProperty("scale")
    private Integer scale;

    @Transient
    private List<String> cleanedUpEnumValues = new ArrayList<>();

    @Column(name = "ENUM_VALUES", nullable = true, length = 2048)
    @JsonProperty("enum_values")
    private String cleanedUpEnumValuesAsString = "";

    @Column(name = "PROPERTIES", nullable = false)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    @AttributePropertyBag
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

    @JsonIgnore
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

    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
    public List<String> getCleanedUpEnumValues() {
        return cleanedUpEnumValues;
    }

    @JsonIgnore
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

    @Transient
    @JsonIgnore
    @Override
    public Object getPropertyValue(String key) {
        return properties.get(key);
    }

    @JsonIgnore
    @Override
    public void setPropertyValue(String key, Object value) {
        properties.put(key, value);
    }

    @JsonIgnore
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

    @JsonIgnore
    private String getGroups() {
        return groups;
    }

    @JsonIgnore
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

    @JsonIgnore
    private Map<ColumnSelection.Predefined, Boolean> getGroupsAsMap() {
        if (StringUtils.isNotBlank(groups)) {
            Map<ColumnSelection.Predefined, Boolean> map = new HashMap<>();
            getGroupsAsList().forEach(g -> map.put(g, true));
            return map;
        } else {
            return null;
        }
    }

    @Transient
    @JsonIgnore
    @Override
    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }

    @Transient
    @Override
    @JsonIgnore
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Transient
    @Override
    @JsonIgnore
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    public Table getTable() {
        return table;
    }

    @JsonIgnore
    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    @JsonIgnore
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonIgnore
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

    @JsonIgnore
    @Transient
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

    @Transient
    @JsonIgnore
    public String getSourceAttrName() {
        return getPropertyValue("SourceAttrName") != null
                ? getPropertyValue("SourceAttrName").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setSourceAttrName(String sourceAttrName) {
        setPropertyValue("SourceAttrName", sourceAttrName);
    }

    @Transient
    @JsonIgnore
    public void setApprovedUsage(String approvedUsage) {
        setListPropertyFromString("ApprovedUsage", approvedUsage);
    }

    @Transient
    @JsonIgnore
    public void setApprovedUsage(ApprovedUsage... approvedUsages) {
        properties.put("ApprovedUsage", getStringValuesFromEnums(approvedUsages));
    }

    @Transient
    @JsonIgnore
    public void setApprovedUsageFromEnumList(List<ApprovedUsage> approvedUsage) {
        properties.put("ApprovedUsage", approvedUsage);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getApprovedUsage() {
        return (List<String>) properties.get("ApprovedUsage");
    }

    @Transient
    @JsonIgnore
    public void setApprovedUsage(List<String> approvedUsage) {
        properties.put("ApprovedUsage", approvedUsage);
    }

    @Transient
    @JsonIgnore
    public boolean isHiddenForRemodelingUI() {
        if (properties.containsKey("IsHiddenForRemodelingUI")) {
            return (Boolean) properties.get("IsHiddenForRemodelingUI");
        }
        return false;
    }

    @Transient
    @JsonIgnore
    public void setIsHiddenForRemodelingUI(boolean isHiddenForRemodelingUI) {
        properties.put("IsHiddenForRemodelingUI", isHiddenForRemodelingUI);
    }

    @Transient
    @JsonIgnore
    public boolean getIsCoveredByOptionalRule() {
        if (properties.containsKey("IsCoveredByOptionalRule")) {
            return (Boolean) properties.get("IsCoveredByOptionalRule");
        }
        return false;
    }

    @Transient
    @JsonIgnore
    public void setIsCoveredByOptionalRule(boolean isCoveredByOptionalRule) {
        properties.put("IsCoveredByOptionalRule", isCoveredByOptionalRule);
    }

    @Transient
    @JsonIgnore
    public boolean getIsCoveredByMandatoryRule() {
        if (properties.containsKey("IsCoveredByMandatoryRule")) {
            return (Boolean) properties.get("IsCoveredByMandatoryRule");
        }
        return false;
    }

    @Transient
    @JsonIgnore
    public void setIsCoveredByMandatoryRule(boolean isCoveredByMandatoryRule) {
        properties.put("IsCoveredByMandatoryRule", isCoveredByMandatoryRule);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public void addAssociatedDataRuleName(String dataRuleName) {
        if (!properties.containsKey("AssociatedDataRules")) {
            properties.put("AssociatedDataRules", new ArrayList<String>());
        }
        ((List<String>) properties.get("AssociatedDataRules")).add(dataRuleName);
    }

    @Transient
    @JsonIgnore
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
    @Transient
    @JsonIgnore
    public void setStatisticalType(String statisticalType) {
        if (StringUtils.isBlank(statisticalType)) {
            properties.remove("StatisticalType");
        } else {
            properties.put("StatisticalType", statisticalType);
        }
    }

    @Transient
    @JsonIgnore
    public String getStatisticalType() {
        return getPropertyValue("StatisticalType") != null
                ? getPropertyValue("StatisticalType").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setStatisticalType(StatisticalType statisticalType) {
        if (statisticalType == null) {
            properties.remove("StatisticalType");
        } else {
            properties.put("StatisticalType", statisticalType.getName());
        }
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setFundamentalType(String fundamentalType) {
        properties.put("FundamentalType", fundamentalType);
    }

    @Transient
    @JsonIgnore
    public String getFundamentalType() {
        return getPropertyValue("FundamentalType") != null
                ? getPropertyValue("FundamentalType").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setFundamentalType(FundamentalType fundamentalType) {
        if (fundamentalType == null) {
            properties.remove("FundamentalType");
        } else {
            properties.put("FundamentalType", fundamentalType.name());
        }
    }

    @Transient
    @JsonIgnore
    public String getDataQuality() {
        return (String) properties.get("DataQuality");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setDataQuality(String dataQuality) {
        properties.put("DataQuality", dataQuality);
    }

    @Transient
    @JsonIgnore
    public void setDataSource(String dataSource) {
        setListPropertyFromString("DataSource", dataSource);
    }

    @SuppressWarnings("unchecked")
    @Transient
    @JsonIgnore
    public List<String> getDataSource() {
        return (List<String>) properties.get("DataSource");
    }

    @Transient
    @JsonIgnore
    public void setDataSource(List<String> dataSource) {
        properties.put("DataSource", dataSource);
    }

    @Transient
    @JsonIgnore
    public String getDisplayDiscretizationStrategy() {
        return (String) properties.get("DisplayDiscretizationStrategy");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setDisplayDiscretizationStrategy(String displayDiscretizationStrategy) {
        properties.put("DisplayDiscretizationStrategy", displayDiscretizationStrategy);
    }

    @Transient
    @JsonIgnore
    public String getDescription() {
        return (String) properties.get("Description");
    }

    @Transient
    @JsonIgnore
    public void setDescription(String description) {
        properties.put("Description", description);
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setTags(String tags) {
        setListPropertyFromString("Tags", tags);
    }

    @Transient
    @JsonIgnore
    public void setTags(Tag... tags) {
        properties.put("Tags", getStringValuesFromEnums(tags));
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getTags() {
        return (List<String>) properties.get("Tags");
    }

    @Transient
    @JsonIgnore
    public void setTags(List<String> tags) {
        properties.put("Tags", tags);
    }

    @Transient
    @JsonIgnore
    public String getPhysicalName() {
        return (String) properties.get("PhysicalName");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setPhysicalName(String physicalName) {
        properties.put("PhysicalName", physicalName);
    }

    @Transient
    @JsonIgnore
    public void setCategory(String category) {
        setPropertyValue("Category", category);
    }

    @Transient
    @JsonIgnore
    public String getCategory() {
        return getPropertyValue("Category") != null ? getPropertyValue("Category").toString()
                : null;
    }

    @Transient
    @JsonIgnore
    public void setCategory(Category category) {
        setPropertyValue("Category", category.getName());
    }

    @Transient
    @JsonIgnore
    public String getSubcategory() {
        return getPropertyValue("Subcategory") != null ? getPropertyValue("Subcategory").toString()
                : null;
    }

    @Transient
    @JsonIgnore
    public void setSubcategory(String subcategory) {
        setPropertyValue("Subcategory", subcategory);
    }

    @Transient
    @JsonIgnore
    public String getDataType() {
        return getPropertyValue("DataType") != null ? getPropertyValue("DataType").toString()
                : null;
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setDataType(String dataType) {
        setPropertyValue("DataType", dataType);
    }

    @Transient
    @JsonIgnore
    public String getRTSModuleName() {
        return (String) properties.get("RTSModuleName");
    }

    @Transient
    @JsonIgnore
    public void setRTSModuleName(String rtsModuleName) {
        properties.put("RTSModuleName", rtsModuleName);
    }

    @Transient
    @JsonIgnore
    public String getRTSArguments() {
        return (String) properties.get("RTSArguments");
    }

    @Transient
    @JsonIgnore
    public void setRTSArguments(String rtsArguments) {
        properties.put("RTSArguments", rtsArguments);
    }

    @Transient
    @JsonIgnore
    public Boolean getRTSAttribute() {
        Boolean rts = (Boolean) properties.get("RTSAttribute");
        if (rts == null) {
            return false;
        }
        return rts;
    }

    @Transient
    @JsonIgnore
    public void setRTSAttribute(String rts) {
        properties.put("RTSAttribute", Boolean.valueOf(rts));
    }

    @Transient
    @JsonIgnore
    public void setRTSAttribute(Boolean rts) {
        properties.put("RTSAttribute", rts);
    }

    @Transient
    @JsonIgnore
    public Boolean getRequired() {
        Boolean required = (Boolean) properties.get("Required");
        if (required == null) {
            return false;
        }
        return required;
    }

    @Transient
    @JsonIgnore
    public void setRequired(Boolean required) {
        properties.put("Required", required);
    }

    @Transient
    @JsonIgnore
    public String getDefaultValueStr() {
        return properties.get("DefaultValueStr") != null
                ? properties.get("DefaultValueStr").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setDefaultValueStr(String defaultValueStr) {
        properties.put("DefaultValueStr", defaultValueStr);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getAllowedDisplayNames() {
        return (List<String>) properties.get("AllowedDisplayNames");
    }

    @JsonIgnore
    @Transient
    public void setAllowedDisplayNames(String allowedDisplayNamesString) {
        setListPropertyFromString("AllowedDisplayNames", allowedDisplayNamesString);
    }

    @Transient
    @JsonIgnore
    public void setAllowedDisplayNames(List<String> allowedDisplayNames) {
        properties.put("AllowedDisplayNames", allowedDisplayNames);
    }

    @JsonIgnore
    @Transient
    public void removeAllowedDisplayNames() {
        properties.remove("AllowedDisplayNames");
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromFiltering() {
        Boolean excludeFromFiltering = (Boolean) properties.get("ExcludeFromFiltering");
        if (excludeFromFiltering == null) {
            return false;
        }
        return excludeFromFiltering;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromFiltering(String excludeFromFiltering) {
        properties.put("ExcludeFromFiltering", Boolean.valueOf(excludeFromFiltering));
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromPlaymakerExport() {
        Boolean excludeFromPlaymakerExport = (Boolean) properties.get("ExcludeFromPlaymakerExport");
        if (excludeFromPlaymakerExport == null) {
            return false;
        }
        return excludeFromPlaymakerExport;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromPlaymakerExport(String excludeFromPlaymakerExport) {
        properties.put("ExcludeFromPlaymakerExport", Boolean.valueOf(excludeFromPlaymakerExport));
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromTalkingPoints() {
        Boolean excludeFromTalkingPoints = (Boolean) properties.get("ExcludeFromTalkingPoints");
        if (excludeFromTalkingPoints == null) {
            return false;
        }
        return excludeFromTalkingPoints;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromTalkingPoints(String excludeFromTalkingPoints) {
        properties.put("ExcludeFromTalkingPoints", Boolean.valueOf(excludeFromTalkingPoints));
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromListView() {
        Boolean excludeFromListView = (Boolean) properties.get("ExcludeFromListView");
        if (excludeFromListView == null) {
            return false;
        }
        return excludeFromListView;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromListView(String excludeFromListView) {
        properties.put("ExcludeFromListView", Boolean.valueOf(excludeFromListView));
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromDetailView() {
        Boolean excludeFromDetailView = (Boolean) properties.get("ExcludeFromDetailView");
        if (excludeFromDetailView == null) {
            return false;
        }
        return excludeFromDetailView;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromDetailView(String excludeFromDetailView) {
        properties.put("ExcludeFromDetailView", Boolean.valueOf(excludeFromDetailView));
    }

    @Transient
    @JsonIgnore
    public Boolean getExcludeFromAll() {
        Boolean excludeFromAll = (Boolean) properties.get("ExcludeFromAll");
        if (excludeFromAll == null) {
            return false;
        }
        return excludeFromAll;
    }

    @JsonIgnore
    @Transient
    public void setExcludeFromAll(String excludeFromAll) {
        properties.put("ExcludeFromAll", Boolean.valueOf(excludeFromAll));
    }

    @Transient
    @JsonIgnore
    public Integer getBitOffset() {
        return (Integer) properties.get("BitOffset");
    }

    @Transient
    @JsonIgnore
    public void setBitOffset(Integer bitOffset) {
        properties.put("BitOffset", bitOffset);
    }

    @Transient
    @JsonIgnore
    public Integer getNumOfBits() {
        return (Integer) properties.get("NumOfBits");
    }

    @Transient
    @JsonIgnore
    public void setNumOfBits(Integer numOfBits) {
        properties.put("NumOfBits", numOfBits);
    }

    @Override
    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
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

    @Transient
    @JsonIgnore
    public ColumnMetadata getColumnMetadata() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setDisplayName(getDisplayName());
        metadata.setSecondaryDisplayName(getSecondaryDisplayName());
        metadata.setAttrName(getName());
        metadata.setDescription(getDescription());
        metadata.setLogicalDataType(getLogicalDataType());
        metadata.setIsHiddenForRemodelingUI(isHiddenForRemodelingUI());
        metadata.setJavaClass(AttributeUtils.toJavaClass(getPhysicalDataType(), getDataType()));
        if (StringUtils.isBlank(getCategory())) {
            metadata.setCategory(Category.DEFAULT);
        } else {
            try {
                metadata.setCategory(Category.fromName(getCategory()));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Cannot parse category " + getCategory() + " for attribute " + getName());
            }

        }
        if (StringUtils.isBlank(getSubcategory())) {
            metadata.setSubcategory("Other");
        } else {
            metadata.setSubcategory(getSubcategory());
        }
        if (StringUtils.isBlank(getFundamentalType())) {
            metadata.setFundamentalType(FundamentalType.ALPHA);
        } else {
            try {
                metadata.setFundamentalType(FundamentalType.fromName(getFundamentalType()));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot parse fundamental type "
                        + getFundamentalType() + " for attribute " + getName());
            }
        }

        if (metadata.getTagList() != null && metadata.getTagList().isEmpty()) {
            metadata.removeTagList();
        }

        if (metadata.getApprovedUsageList() != null && metadata.getApprovedUsageList().isEmpty()) {
            metadata.removeApprovedUsageList();
        }

        if (MapUtils.isNotEmpty(getGroupsAsMap())) {
            metadata.setGroups(getGroupsAsMap());
        }

        metadata.setStatisticalType(StatisticalType.fromName(getStatisticalType()));
        metadata.setDiscretizationStrategy(getDisplayDiscretizationStrategy());
        metadata.setBitOffset(getBitOffset());
        metadata.setNumBits(getNumOfBits());
        metadata.setPhysicalName(getPhysicalName());
        if (CollectionUtils.isNotEmpty(getApprovedUsage())) {
            metadata.setApprovedUsageList(getApprovedUsage().stream().map(ApprovedUsage::fromName)
                    .collect(Collectors.toList()));
        }
        metadata.setIsCoveredByMandatoryRule(getIsCoveredByMandatoryRule());
        metadata.setIsCoveredByOptionalRule(getIsCoveredByOptionalRule());
        metadata.setAssociatedDataRules(getAssociatedDataRules());
        return metadata;
    }

    @Transient
    @JsonIgnore
    public String getDateTimeFormatString() {
        Object raw = properties.get("DateTimeFormatString");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    @Transient
    @JsonIgnore
    public void setDateTimeFormatString(String dateTimeFormatString) {
        if (dateTimeFormatString != null) {
            properties.put("DateTimeFormatString", dateTimeFormatString);
        }
    }

    @Transient
    @JsonIgnore
    public String getTimezone() {
        Object raw = properties.get("Timezone");
        if (raw == null) {
            return null;
        }

        return raw.toString();
    }

    @Transient
    @JsonIgnore
    public void setTimezone(String timezone) {
        if (timezone != null) {
            properties.put("Timezone", timezone);
        }
    }

}

