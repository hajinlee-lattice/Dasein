package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PLAY")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Play implements HasName, HasPid, HasTenantId {

    public static final String PLAY_NAME_PREFIX = "play";
    public static final String PLAY_NAME_FORMAT = "%s__%s";

    public Play() {
        setName(generateNameStr());
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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
    @Column(name = "DESCRIPTION", nullable = true)
    private String description;

    @JsonProperty("segment")
    @Column(name = "SEGMENT_NAME", nullable = true)
    private String segmentName;

    @JsonIgnore
    @Transient
    private MetadataSegment segment;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_CALL_PREP_ID")
    private CallPrep callPrep;

    @OneToMany(fetch = FetchType.EAGER)
    @JsonProperty("rating")
    private List<RatingObject> rating = new ArrayList<>();

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("timestamp")
    @Column(name = "TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date timestamp;

    @JsonProperty("lastupdatedtimestamp")
    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastUpdatedTimestamp;

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY", nullable = false)
    private String createdBy;

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

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
        if (segment != null) {
            setSegmentName(segment.getName());
        }
    }

    public CallPrep getCallPrep() {
        return callPrep;
    }

    public void setCallPrep(CallPrep callPrep) {
        this.callPrep = callPrep;
    }


    public List<RatingObject> getRating() {
        return rating;
    }

    public void setRating(List<RatingObject> rating) {
        this.rating = rating;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return this.tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    @JsonIgnore
    public Long getTenantId() {
        return this.tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getLastUpdatedTimestamp() {
        return this.lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String generateNameStr() {
        return String.format(PLAY_NAME_FORMAT, PLAY_NAME_PREFIX, UUID.randomUUID().toString());
    }
}
