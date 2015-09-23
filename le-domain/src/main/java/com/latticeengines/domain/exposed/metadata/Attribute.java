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
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

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
@Table(name = "METADATA_ATTRIBUTE")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
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
    private AttributeOwner attributeOwner;
    private Tenant tenant;
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

    @Column(name = "NAME", unique = true, nullable = false)
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

    @Column(name = "LENGTH", nullable = false)
    @JsonIgnore
    public Integer getLength() {
        return length;
    }

    @JsonIgnore
    public void setLength(Integer length) {
        this.length = length;
    }

    @Column(name = "NULLABLE", nullable = false)
    @JsonIgnore
    public Boolean isNullable() {
        return nullable;
    }

    @JsonIgnore
    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @Column(name = "DATA_TYPE", nullable = false)
    @JsonIgnore
    public String getPhysicalDataType() {
        return physicalDataType;
    }

    @JsonIgnore
    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    @Column(name = "LOGICAL_DATA_TYPE", nullable = false)
    @JsonProperty("logical_type")
    public String getLogicalDataType() {
        return logicalDataType;
    }

    @JsonProperty("logical_type")
    public void setLogicalDataType(String logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    @Column(name = "PRECISION", nullable = false)
    @JsonIgnore
    public Integer getPrecision() {
        return precision;
    }

    @JsonIgnore
    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    @Column(name = "SCALE", nullable = false)
    @JsonIgnore
    public Integer getScale() {
        return scale;
    }

    @JsonIgnore
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

    @Column(name = "ENUM_VALUES", nullable = true)
    public String getCleanedUpEnumValuesAsString() {
        return StringUtils.join(cleanedUpEnumValues, ",");
    }

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

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_ATTRIBUTE_OWNER_ID", nullable = false)
    public AttributeOwner getAttributeOwner() {
        return attributeOwner;
    }

    public void setAttributeOwner(AttributeOwner attributeOwner) {
        this.attributeOwner = attributeOwner;
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
        this.tenant = tenant;
        setTenantId(tenant.getPid());
    }

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    @Transient
    @JsonIgnore
    public String toString() {
        return name;
    }

}
