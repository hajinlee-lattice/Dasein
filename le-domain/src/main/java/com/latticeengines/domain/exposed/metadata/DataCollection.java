package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_DATA_COLLECTION", uniqueConstraints = @UniqueConstraint(columnNames = {
        "TENANT_ID", "NAME" }))
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCollection extends BaseMetadataPropertyOwner<DataCollectionProperty>
        implements HasName, HasTenant, HasTenantId, HasPid, HasProperties<DataCollectionProperty> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @OneToMany(cascade = CascadeType.MERGE, fetch = FetchType.LAZY, mappedBy = "owner")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("properties")
    private List<DataCollectionProperty> properties = new ArrayList<>();

    @OneToMany(cascade = CascadeType.MERGE, fetch = FetchType.LAZY, mappedBy = "dataCollection")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private List<DataFeed> datafeeds = new ArrayList<>();

    @OneToMany(cascade = CascadeType.MERGE, fetch = FetchType.LAZY, mappedBy = "dataCollection")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private List<MetadataSegment> segments = new ArrayList<>();

    @JsonProperty("type")
    @Enumerated(EnumType.STRING)
    @Column(name = "TYPE", nullable = false)
    private DataCollectionType type;

    @OneToMany(cascade = CascadeType.MERGE, fetch = FetchType.LAZY, mappedBy = "dataCollection")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private List<DataCollectionTable> collectionTables = new ArrayList<>();

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

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
        this.tenant = tenant;
    }

    public DataCollectionType getType() {
        return type;
    }

    public void setType(DataCollectionType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }


    @Override
    public List<DataCollectionProperty> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(List<DataCollectionProperty> properties) {
        this.properties = properties;
    }

    // =====
    // Use datafeed entity mgr
    private List<DataFeed> getDatafeeds() {
        return datafeeds;
    }

    private void setDatafeeds(List<DataFeed> datafeeds) {
        this.datafeeds = datafeeds;
    }
    // =====

    // =====
    // Use segment entity mgr
    private List<MetadataSegment> getSegments() {
        return segments;
    }

    private void setSegments(List<MetadataSegment> segments) {
        this.segments = segments;
    }
    // =====

    @JsonIgnore
    @Transient
    public DataCollectionPropertyBag getDataCollectionPropertyBag() {
        return new DataCollectionPropertyBag(properties);
    }

    @Override
    @JsonIgnore
    protected Class<DataCollectionProperty> getPropertyClz() {
        return DataCollectionProperty.class;
    }

}
