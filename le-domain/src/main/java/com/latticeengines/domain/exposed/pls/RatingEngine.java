package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "RATING_ENGINE")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngine implements HasPid, HasId<String>, HasTenant, HasAuditingFields {

    public static final String RATING_ENGINE_PREFIX = "rating_engine";
    public static final String RATING_ENGINE_FORMAT = "%s__%s";

    public RatingEngine() {
    }

    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("id")
    @Column(name = "ID", unique = true, nullable = false)
    private String id;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("displayName")
    @Column(name = "DISPLAY_NAME", nullable = true)
    private String displayName;

    @JsonProperty("note")
    @Column(name = "note", nullable = true)
    @Type(type = "text")
    private String note;

    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private RatingEngineType type;

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private RatingEngineStatus status = RatingEngineStatus.INACTIVE;

    @JsonProperty("segment")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment segment;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY", nullable = false)
    private String createdBy;

    @JsonProperty("ratingModels")
    @OneToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE,
            CascadeType.MERGE }, mappedBy = "ratingEngine", fetch = FetchType.EAGER, orphanRemoval = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Set<RatingModel> ratingModels = new HashSet<>();

    @JsonIgnore
    @OneToMany(cascade = { CascadeType.MERGE }, mappedBy = "ratingEngine", fetch = FetchType.LAZY)
    private Set<Play> plays;

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    @Override
    public Date getUpdated() {
        return this.updated;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getNote() {
        return this.note;
    }

    public void setType(RatingEngineType type) {
        this.type = type;
    }

    public RatingEngineType getType() {
        return this.type;
    }

    public void setStatus(RatingEngineStatus status) {
        this.status = status;
    }

    public RatingEngineStatus getStatus() {
        return this.status;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }

    public MetadataSegment getSegment() {
        return this.segment;
    }

    public void setCreatedBy(String user) {
        this.createdBy = user;
    }

    public String getCreatedBy() {
        return this.createdBy;
    }

    public void setRatingModels(Set<RatingModel> ratingModels) {
        this.ratingModels = ratingModels;
    }

    public Set<RatingModel> getRatingModels() {
        return this.ratingModels;
    }

    public void addRatingModel(RatingModel ratingModel) {
        if (this.ratingModels == null) {
            this.ratingModels = new HashSet<>();
        }
        ratingModel.setRatingEngine(this);
        this.ratingModels.add(ratingModel);
    }

    public void setPlays(Set<Play> plays) {
        this.plays = plays;
    }

    public Set<Play> getPlays() {
        return this.plays;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public static String generateIdStr() {
        return String.format(RATING_ENGINE_FORMAT, RATING_ENGINE_PREFIX, UuidUtils.shortenUuid(UUID.randomUUID()));
    }
}
