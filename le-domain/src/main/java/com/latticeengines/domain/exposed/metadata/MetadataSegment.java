package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import javax.persistence.OneToMany;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.ApiModelProperty;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT", uniqueConstraints = @UniqueConstraint(columnNames = {
        "TENANT_ID", "NAME" }))
@JsonIgnoreProperties(ignoreUnknown = true)
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
public class MetadataSegment extends BaseMetadataPropertyOwner<MetadataSegmentProperty> //
        implements HasName, HasPid, HasAuditingFields, HasTenantId, HasProperties<MetadataSegmentProperty> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", length = 1000)
    private String description;

    @JsonIgnore
    @Column(name = "RESTRICTION")
    @Type(type = "text")
    private String restrictionString;

    @JsonProperty("frontend_restriction")
    @Transient
    @ApiModelProperty("Restriction for use in the front end")
    private FrontEndRestriction frontEndRestriction;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    private Date updated;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    private Date created;

    @JsonProperty("is_master_segment")
    @Column(name = "IS_MASTER_SEGMENT", nullable = false)
    private Boolean isMasterSegment = false;

    @OneToMany(cascade = CascadeType.MERGE, mappedBy = "owner", fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("segment_properties")
    private List<MetadataSegmentProperty> properties = new ArrayList<>();

    @Transient
    @JsonProperty("attributes")
    private List<Attribute> attributeDependencies = new ArrayList<>();

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("restriction")
    public Restriction getRestriction() {
        return JsonUtils.deserialize(restrictionString, Restriction.class);
    }

    @JsonProperty("restriction")
    public void setRestriction(Restriction restriction) {
        this.restrictionString = JsonUtils.serialize(restriction);
    }

    public FrontEndRestriction getFrontEndRestriction() {
        return frontEndRestriction;
    }

    public void setFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        this.frontEndRestriction = frontEndRestriction;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    @Override
    public List<MetadataSegmentProperty> getProperties() {
        return properties;
    }

    @Override
    protected void setProperties(List<MetadataSegmentProperty> properties) {
        this.properties = properties;
    }

    @Transient
    @JsonIgnore
    public MetadataSegmentPropertyBag getSegmentPropertyBag() {
        return new MetadataSegmentPropertyBag(properties);
    }

    @Transient
    @JsonIgnore
    public void setSegmentPropertyBag(MetadataSegmentPropertyBag properties) {
        this.properties = properties.getBag();
    }

    public void addSegmentProperty(MetadataSegmentProperty metadataSegmentProperty) {
        this.putProperty(metadataSegmentProperty.getOption(), metadataSegmentProperty.getValue());
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

    public List<Attribute> getAttributeDependencies() {
        return attributeDependencies;
    }

    public void setAttributeDependencies(List<Attribute> attributeDependencies) {
        this.attributeDependencies = attributeDependencies;
    }

    public Boolean getMasterSegment() {
        return isMasterSegment;
    }

    public void setMasterSegment(Boolean masterSegment) {
        isMasterSegment = masterSegment;
    }

    @Override
    @JsonIgnore
    protected Class<MetadataSegmentProperty> getPropertyClz() {
        return MetadataSegmentProperty.class;
    }

}
