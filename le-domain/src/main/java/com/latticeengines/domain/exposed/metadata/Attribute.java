package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasProperty;
import com.latticeengines.domain.exposed.metadata.annotation.AttributePropertyBag;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_ATTRIBUTE")
public class Attribute implements HasName, HasPid, HasProperty, HasTenantId, Serializable, GraphNode {

    private static final long serialVersionUID = -4779448415471374224L;

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

    @Column(name = "LENGTH", nullable = true)
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
    private List<String> cleanedUpEnumValues = new ArrayList<String>();

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

    @Column(name = "VALIDATORS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private List<InputValidatorWrapper> validatorWrappers = new ArrayList<>();

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

    public void setLogicalDataType(LogicalDataType logicalDataType) {
        this.logicalDataType = logicalDataType;
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
    public void setInterfaceName(InterfaceName interfaceName) {
        if (interfaceName != null) {
            properties.put("InterfaceName", interfaceName.toString());
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
                    setPropertyValue(key, Lists.newArrayList());
                } else {
                    String[] array = contents.split(",");
                    for (int i = 0; i < array.length; ++i) {
                        array[i] = array[i].trim();
                    }
                    setPropertyValue(key, Arrays.asList(array));
                }
            } else {
                setPropertyValue(key, Arrays.asList(value));
            }
        } else {
            setPropertyValue(key, Arrays.asList(value));
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
    public void setApprovedUsage(List<String> approvedUsage) {
        properties.put("ApprovedUsage", approvedUsage);
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
    public void setIsCoveredByOptionalRule(boolean isCoveredByOptionalRule) {
        properties.put("IsCoveredByOptionalRule", isCoveredByOptionalRule);
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
    public void setIsCoveredByMandatoryRule(boolean isCoveredByMandatoryRule) {
        properties.put("IsCoveredByMandatoryRule", isCoveredByMandatoryRule);
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
            return new ArrayList<String>();
        }
        return (List<String>) properties.get("AssociatedDataRules");
    }

    /**
     * Used for VisiDB/legacy systems
     */
    @Transient
    @JsonIgnore
    public void setStatisticalType(String statisticalType) {
        properties.put("StatisticalType", statisticalType);
    }

    @Transient
    @JsonIgnore
    public void setStatisticalType(StatisticalType statisticalType) {
        properties.put("StatisticalType", statisticalType.getName());
    }

    @Transient
    @JsonIgnore
    public String getStatisticalType() {
        return (String) properties.get("StatisticalType");
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
    public void setFundamentalType(FundamentalType fundamentalType) {
        properties.put("FundamentalType", fundamentalType.getName());
    }

    @Transient
    @JsonIgnore
    public String getFundamentalType() {
        return (String) properties.get("FundamentalType");
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
    public String getDataQuality() {
        return (String) properties.get("DataQuality");
    }

    @Transient
    @JsonIgnore
    public void setDataSource(String dataSource) {
        setListPropertyFromString("DataSource", dataSource);
    }

    @Transient
    @JsonIgnore
    public void setDataSource(List<String> dataSource) {
        properties.put("DataSource", dataSource);
    }

    @SuppressWarnings("unchecked")
    @Transient
    @JsonIgnore
    public List<String> getDataSource() {
        return (List<String>) properties.get("DataSource");
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
    public String getDisplayDiscretizationStrategy() {
        return (String) properties.get("DisplayDiscretizationStrategy");
    }

    @Transient
    @JsonIgnore
    public void setDescription(String description) {
        properties.put("Description", description);
    }

    @Transient
    @JsonIgnore
    public String getDescription() {
        return (String) properties.get("Description");
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
    public void setTags(List<String> tags) {
        properties.put("Tags", tags);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getTags() {
        return (List<String>) properties.get("Tags");
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
    public String getPhysicalName() {
        return (String) properties.get("PhysicalName");
    }

    @Transient
    @JsonIgnore
    public void setCategory(String category) {
        setPropertyValue("Category", category);
    }

    @Transient
    @JsonIgnore
    public void setCategory(Category category) {
        setPropertyValue("Category", category.getName());
    }

    @Transient
    @JsonIgnore
    public String getCategory() {
        return getPropertyValue("Category") != null ? getPropertyValue("Category").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setSubcategory(String subcategory) {
        setPropertyValue("Subcategory", subcategory);
    }

    @Transient
    @JsonIgnore
    public String getSubcategory() {
        return getPropertyValue("Subcategory") != null ? getPropertyValue("Subcategory").toString() : null;
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
    public String getDataType() {
        return getPropertyValue("DataType") != null ? getPropertyValue("DataType").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setRTSModuleName(String rtsModuleName) {
        properties.put("RTSModuleName", rtsModuleName);
    }

    @Transient
    @JsonIgnore
    public String getRTSModuleName() {
        return (String) properties.get("RTSModuleName");
    }

    @Transient
    @JsonIgnore
    public void setRTSArguments(String rtsArguments) {
        properties.put("RTSArguments", rtsArguments);
    }

    @Transient
    @JsonIgnore
    public String getRTSArguments() {
        return (String) properties.get("RTSArguments");
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
    public void setRTSAttribute(Boolean rts) {
        properties.put("RTSAttribute", rts);
    }

    @Transient
    @JsonIgnore
    public void setRTSAttribute(String rts) {
        properties.put("RTSAttribute", Boolean.valueOf(rts));
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getAllowedDisplayNames() {
        return (List<String>) properties.get("AllowedDisplayNames");
    }

    @Transient
    @JsonIgnore
    public void setAllowedDisplayNames(List<String> allowedDisplayNames) {
        properties.put("AllowedDisplayNames", allowedDisplayNames);
    }

    @JsonIgnore
    @Transient
    public void setAllowedDisplayNames(String allowedDisplayNamesString) {
        setListPropertyFromString("AllowedDisplayNames", allowedDisplayNamesString);
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
        properties.put("xcludeFromPlaymakerExport", Boolean.valueOf(excludeFromPlaymakerExport));
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
    public void setBitOffset(Integer bitOffset) {
        properties.put("BitOffset", bitOffset);
    }

    @Transient
    @JsonIgnore
    public Integer getBitOffset() {
        return (Integer) properties.get("BitOffset");
    }

    @Transient
    @JsonIgnore
    public void setNumOfBits(Integer numOfBits) {
        properties.put("NumOfBits", numOfBits);
    }

    @Transient
    @JsonIgnore
    public Integer getNumOfBits() {
        return (Integer) properties.get("NumOfBits");
    }

    @Transient
    @JsonIgnore
    public void setBucketRangeList(List<BucketRange> bucketRangeList) {
        properties.put("BucketRangeList", bucketRangeList);
    }

    @Transient
    @JsonIgnore
    public void addBucketRange(BucketRange bucketRange) {
        List<BucketRange> bucketRangeList = getBucketRangeList();
        if (bucketRangeList == null) {
            bucketRangeList = new ArrayList<>();
        }
        bucketRangeList.add(bucketRange);
        setBucketRangeList(bucketRangeList);
    }

    @Transient
    @JsonIgnore
    public void addBucketedValue(String bucketRangeStr) {
        if (bucketRangeStr == null || bucketRangeStr.equals("null")) {
            addBucketRange(BucketRange.nullBucket());
        } else {
            addBucketRange(BucketRange.value(bucketRangeStr));
        }
    }

    @Transient
    @JsonIgnore
    public List<BucketRange> getBucketRangeList() {
        Object obj = properties.get("BucketRangeList");
        return obj == null ? null : JsonUtils.getObjectMapper().convertValue(obj,
                new TypeReference<List<BucketRange>>() {
                });
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
                if (tag.equals(Tag.INTERNAL.toString()) || tag.equals(Tag.INTERNAL_TRANSFORM.toString())) {
                    isInternalAttributeOrTransform = true;
                    break;
                }
            }
        }
        if (!isInternalAttributeOrTransform) {
            return false;
        }
        if (getInterfaceName() != null)
            return false;
        return true;
    }

    @Transient
    @JsonIgnore
    public ColumnMetadata getColumnMetadata() {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setDisplayName(getDisplayName());
        metadata.setColumnName(getName());
        metadata.setColumnId(getName());
        metadata.setDescription(getDescription());
        metadata.setCategory(Category.fromName(getCategory()));
        metadata.setSubcategory(getSubcategory());
        metadata.setDataType(getDataType());
        metadata.setFundamentalType(FundamentalType.fromName(getFundamentalType()));
        metadata.setStatisticalType(StatisticalType.fromName(getStatisticalType()));
        metadata.setDiscretizationStrategy(getDisplayDiscretizationStrategy());
        return metadata;
    }

}
