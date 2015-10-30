package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
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
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasProperty;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_ATTRIBUTE")
public class Attribute implements HasName, HasPid, HasProperty, HasTenantId, Serializable, GraphNode {

    private static final long serialVersionUID = -4779448415471374224L;

    private Long pid;
    private String name;
    private String displayName;
    private Integer length;
    private Boolean nullable = Boolean.FALSE;
    private String physicalDataType;
    private String logicalDataType;
    private Integer precision;
    private Integer scale;
    private List<String> cleanedUpEnumValues = new ArrayList<String>();
    private List<String> enumValues = new ArrayList<String>();
    private Map<String, Object> properties = new HashMap<>();
    private Table table;
    private Long tenantId;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Column(name = "NAME", nullable = false)
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Column(name = "LENGTH", nullable = true)
    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    @Column(name = "NULLABLE", nullable = false)
    public Boolean isNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @Column(name = "DATA_TYPE", nullable = false)
    public String getPhysicalDataType() {
        return physicalDataType;
    }

    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    @Column(name = "LOGICAL_DATA_TYPE", nullable = true)
    @JsonProperty("logical_type")
    public String getLogicalDataType() {
        return logicalDataType;
    }

    @JsonProperty("logical_type")
    public void setLogicalDataType(String logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    @Column(name = "PRECISION", nullable = true)
    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    @Column(name = "SCALE", nullable = true)
    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    @JsonIgnore
    @Transient
    public List<String> getEnumValues() {
        return enumValues;
    }

    @JsonIgnore
    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }

    @Transient
    @JsonIgnore
    public List<String> getCleanedUpEnumValues() {
        return cleanedUpEnumValues;
    }

    @JsonIgnore
    public void setCleanedUpEnumValues(List<String> cleanedUpEnumValues) {
        this.cleanedUpEnumValues = cleanedUpEnumValues;
    }

    @Column(name = "ENUM_VALUES", nullable = true, length = 2048)
    @JsonProperty("enum_values")
    public String getCleanedUpEnumValuesAsString() {
        return StringUtils.join(cleanedUpEnumValues, ",");
    }

    @JsonProperty("enum_values")
    public void setCleanedUpEnumValuesAsString(String enumValues) {
        if (enumValues != null) {
            setCleanedUpEnumValues(Arrays.<String>asList(enumValues.split(",")));
        }
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

    @Transient
    @JsonIgnore
    @Override
    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
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

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    public Table getTable() {
        return table;
    }

    @JsonIgnore
    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
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

    @Column(name = "PROPERTIES", nullable = false)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    public Map<String, Object> getProperties() {
        return properties;
    }
    
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
    
    @Transient
    @JsonIgnore
    public void setApprovedUsage(String approvedUsage) {
        setApprovedUsage(Arrays.<String>asList(new String[] { approvedUsage }));
    }
    
    @Transient
    @JsonIgnore
    public void setApprovedUsage(List<String> approvedUsage) {
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
    public void setStatisticalType(String statisticalType) {
        properties.put("StatisticalType", statisticalType);
    }
    
    @Transient
    @JsonIgnore
    public String getStatisticalType() {
        return (String) properties.get("StatisticalType");
    }

    @Transient
    @JsonIgnore
    public void setFundamentalType(String fundamentalType) {
        properties.put("FundamentalType", fundamentalType);
    }
    
    @Transient
    @JsonIgnore
    public String getFundamentalType() {
        return (String) properties.get("FundamentalType");
    }

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
        setDataSource(Arrays.<String>asList(new String[] { dataSource }));
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
    
    @Transient
    @JsonIgnore
    public void setTags(String tags) {
        setTags(Arrays.<String>asList(new String[] { tags }));
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
    

    @Transient
    @JsonIgnore
    public void setCategory(String category) {
        setExtensionValue("Category", category);
    }
    
    @Transient
    @JsonIgnore
    public String getCategory() {
        return getExtensionValue("Category");
    }
    
    @Transient
    @JsonIgnore
    public void setDataType(String dataType) {
        setExtensionValue("DataType", dataType);
    }
    
    @Transient
    @JsonIgnore
    public String getDataType() {
        return getExtensionValue("DataType");
    }
    
    @SuppressWarnings({ "unchecked" })
    private void setExtensionValue(String key, String value) {
        List<Map<String, String>> entries = (List<Map<String, String>>) properties.get("Extensions");
        boolean keyFound = false;
        if (entries != null) {
            for (Map<String, String> entry : entries) {
                if (entry.containsKey(key)) {
                    keyFound = true;
                    entry.put(key, value);
                }
            }
        } else {
            entries = new ArrayList<>();
        }
        if (!keyFound) {
            Map<String, String> map = new HashMap<>();
            map.put(key, value);
            entries.add(map);
            properties.put("Extensions", entries);
        }
    }
    
    @SuppressWarnings({ "unchecked" })
    private String getExtensionValue(String key) {
        List<Map<String, String>> entries = (List<Map<String, String>>) properties.get("Extensions");
        
        if (entries != null) {
            for (Map<String, String> entry : entries) {
                if (entry.containsKey(key)) {
                    return entry.get(key);
                }
            }
        }
        return null;
    }

    @Override
    @Transient
    @JsonIgnore
    public String toString() {
        return name;
    }

}
