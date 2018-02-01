package com.latticeengines.domain.exposed.pls;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import javax.persistence.Transient;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
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

    public static final String RATING_ENGINE_PREFIX = "engine";
    public static final String RATING_ENGINE_FORMAT = "%s_%s";
    public static final String DEFAULT_NAME_PATTERN = "RATING ENGINE -- %s";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

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
    @Transient
    private String note;

    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private RatingEngineType type;

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private RatingEngineStatus status;

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

    @JsonIgnore
    @Column(name = "ACTIVE_MODEL_PID")
    private Long activeModelPid;

    @JsonProperty("activeModel")
    @Transient
    private RatingModel activeModel;

    @JsonIgnore
    @OneToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.REFRESH,
            CascadeType.MERGE }, mappedBy = "ratingEngine", fetch = FetchType.LAZY, orphanRemoval = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<RatingEngineNote> ratingEngineNotes;

    @Column(name = "COUNTS", length = 1000)
    @JsonIgnore
    private String counts;

    @JsonProperty("lastRefreshedDate")
    @Transient
    private Date lastRefreshedDate;

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

    public Date getLastRefreshedDate() {
        return lastRefreshedDate;
    }

    public void setLastRefreshedDate(Date lastRefreshedDate) {
        this.lastRefreshedDate = lastRefreshedDate;
    }

    @SuppressWarnings("unused")
    private String getCounts() {
        return counts;
    }

    private void setCounts(String counts) {
        this.counts = counts;
    }

    @JsonProperty("counts")
    public Map<String, Long> getCountsAsMap() {
        if (StringUtils.isBlank(counts)) {
            return null;
        }
        Map map = JsonUtils.deserialize(counts, Map.class);
        return JsonUtils.convertMap(map, String.class, Long.class);
    }

    @JsonProperty("counts")
    public void setCountsByMap(Map<String, Long> countMap) {
        if (MapUtils.isEmpty(countMap)) {
            counts = null;
        } else {
            counts = JsonUtils.serialize(countMap);
        }
    }

    public List<RatingEngineNote> getRatingEngineNotes() {
        return this.ratingEngineNotes;
    }

    public void setRatingEngineNotes(List<RatingEngineNote> ratingEngineNotes) {
        this.ratingEngineNotes = ratingEngineNotes;
    }

    public void addRatingEngineNote(RatingEngineNote ratingEngineNote) {
        if (this.ratingEngineNotes == null) {
            this.ratingEngineNotes = new ArrayList<>();
        }
        ratingEngineNote.setRatingEngine(this);
        this.ratingEngineNotes.add(ratingEngineNote);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public RatingModel getActiveModel() {
        return this.activeModel;
    }

    public void setActiveModel(RatingModel model) {
        this.activeModel = model;
    }

    public Long getActiveModelPid() {
        return this.activeModelPid;
    }

    public void setActiveModelPid(Long pid) {
        this.activeModelPid = pid;
    }

    public static String generateIdStr() {
        String uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        return String.format(RATING_ENGINE_FORMAT, RATING_ENGINE_PREFIX, uuid);
    }

    public static String toRatingAttrName(String engineId) {
        if (engineId.startsWith(RATING_ENGINE_PREFIX)) {
            return engineId;
        } else {
            return String.format(RATING_ENGINE_FORMAT, RATING_ENGINE_PREFIX, engineId);
        }
    }
}
