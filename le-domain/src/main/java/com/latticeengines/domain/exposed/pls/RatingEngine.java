package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;
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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.cdl.rating.AdvancedRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.CrossSellRatingConfig;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "RATING_ENGINE")
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDefs({
        @FilterDef(name = "tenantFilter", defaultCondition = "FK_TENANT_ID = :tenantFilterId", parameters = {
                @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }),
        @FilterDef(name = "softDeleteFilter", defaultCondition = "DELETED !=true") })
@Filters({ @Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId"),
        @Filter(name = "softDeleteFilter", condition = "DELETED != true") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngine implements HasPid, HasId<String>, HasTenant, HasAuditingFields, SoftDeletable {

    public static final String RATING_ENGINE_PREFIX = "engine";
    public static final String RATING_ENGINE_FORMAT = "%s_%s";
    public static final String DEFAULT_NAME_PATTERN = "New Model - %s";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public static final Map<ScoreType, String> SCORE_ATTR_SUFFIX = ImmutableMap.of( //
            ScoreType.Probability, "prob", //
            ScoreType.ExpectedRevenue, "ev", //
            ScoreType.PredictedRevenue, "pv", //
            ScoreType.Score, "score" //
    );
    public static final Map<ScoreType, Class<? extends Serializable>> SCORE_ATTR_CLZ = ImmutableMap.of( //
            ScoreType.Probability, Double.class, //
            ScoreType.ExpectedRevenue, Double.class, //
            ScoreType.PredictedRevenue, Double.class, //
            ScoreType.Score, Integer.class //
    );
    private static final Logger log = LoggerFactory.getLogger(RatingEngine.class);
    private static final String RULES_BASED_NAME_PATTERN = "Rules %s - %s";
    private static final String CROSS_SELL_NAME_PATTERN = "%s Purchase - %s";
    private static final String CUSTOM_EVENT_NAME_PATTERN = "Custom Event - %s";
    private Long pid;

    private String id;

    private Tenant tenant;

    private String displayName;

    private String description;

    private String note;

    private RatingEngineType type;

    private RatingEngineStatus status;

    private Boolean deleted = false;

    private MetadataSegment segment;

    private Date created;

    private Date updated;

    private String createdBy;

    private String updatedBy;

    private List<RatingEngineNote> ratingEngineNotes;

    private Map<String, Long> countMap;

    private Date lastRefreshedDate;

    private AdvancedRatingConfig advancedRatingConfig;

    private List<BucketMetadata> bucketMetadata;

    private RatingModel latestIteration;

    private RatingModel scoringIteration;

    private RatingModel publishedIteration;

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

    public static String toRatingAttrName(String engineId, ScoreType scoreType) {
        String attr = toRatingAttrName(engineId);
        if (!ScoreType.Rating.equals(scoreType)) {
            attr += "_" + SCORE_ATTR_SUFFIX.get(scoreType);
        }
        return attr;
    }

    public static String toEngineId(String attrName) {
        String uuid = attrName.replace(RATING_ENGINE_PREFIX + "_", "");
        uuid = uuid.substring(0, 22);
        return String.format(RATING_ENGINE_FORMAT, RATING_ENGINE_PREFIX, uuid);
    }

    @Override
    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    @JsonProperty("pid")
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("id")
    @Column(name = "ID", unique = true, nullable = false)
    public String getId() {
        return this.id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @JsonProperty("displayName")
    @Column(name = "DISPLAY_NAME", nullable = true)
    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", length = 1000)
    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    public Date getCreated() {
        return this.created;
    }

    @Override
    @JsonProperty("created")
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    public Date getUpdated() {
        return this.updated;
    }

    @Override
    @JsonProperty("updated")
    public void setUpdated(Date time) {
        this.updated = time;
    }

    @JsonProperty("note")
    @Transient
    public String getNote() {
        return this.note;
    }

    @JsonProperty("note")
    public void setNote(String note) {
        this.note = note;
    }

    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    public RatingEngineType getType() {
        return this.type;
    }

    @JsonProperty("type")
    public void setType(RatingEngineType type) {
        this.type = type;
    }

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    public RatingEngineStatus getStatus() {
        return this.status;
    }

    @JsonProperty("status")
    public void setStatus(RatingEngineStatus status) {
        this.status = status;
    }

    @Override
    @JsonProperty("deleted")
    @Column(name = "DELETED")
    public Boolean getDeleted() {
        return this.deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    @JsonProperty("justCreated")
    @Transient
    public Boolean getJustCreated() {
        return scoringIteration == null;
    }

    @JsonProperty("segment")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_SEGMENT_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public MetadataSegment getSegment() {
        return this.segment;
    }

    @JsonProperty("segment")
    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY", nullable = false)
    public String getCreatedBy() {
        return this.createdBy;
    }

    @JsonProperty("createdBy")
    public void setCreatedBy(String user) {
        this.createdBy = user;
    }

    @JsonProperty("updatedBy")
    @Column(name = "UPDATED_BY", nullable = false)
    public String getUpdatedBy() {
        return this.updatedBy;
    }

    @JsonProperty("updatedBy")
    public void setUpdatedBy(String user) {
        this.updatedBy = user;
    }

    @JsonProperty("lastRefreshedDate")
    @Transient
    public Date getLastRefreshedDate() {
        return lastRefreshedDate;
    }

    @JsonProperty("lastRefreshedDate")
    public void setLastRefreshedDate(Date lastRefreshedDate) {
        this.lastRefreshedDate = lastRefreshedDate;
    }

    @Column(name = "COUNTS", length = 1000)
    public String getCountsStr() {
        String counts = null;
        if (!MapUtils.isEmpty(countMap)) {
            counts = JsonUtils.serialize(countMap);
        }
        return counts;
    }

    public void setCountsStr(String counts) {
        if (!StringUtils.isBlank(counts)) {
            Map<?, ?> map = JsonUtils.deserialize(counts, Map.class);
            this.countMap = JsonUtils.convertMap(map, String.class, Long.class);
        } else {
            this.countMap = null;
        }
    }

    @JsonProperty("counts")
    @Transient
    public Map<String, Long> getCountsAsMap() {
        return countMap;
    }

    @JsonProperty("counts")
    public void setCountsByMap(Map<String, Long> countMap) {
        this.countMap = countMap;
    }

    @JsonIgnore
    @OneToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.REFRESH,
            CascadeType.MERGE }, mappedBy = "ratingEngine", fetch = FetchType.LAZY, orphanRemoval = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<RatingEngineNote> getRatingEngineNotes() {
        return this.ratingEngineNotes;
    }

    @JsonIgnore
    public void setRatingEngineNotes(List<RatingEngineNote> ratingEngineNotes) {
        this.ratingEngineNotes = ratingEngineNotes;
    }

    @Transient
    @JsonIgnore
    public void addRatingEngineNote(RatingEngineNote ratingEngineNote) {
        if (this.ratingEngineNotes == null) {
            this.ratingEngineNotes = new ArrayList<>();
        }
        ratingEngineNote.setRatingEngine(this);
        this.ratingEngineNotes.add(ratingEngineNote);
    }

    @JsonProperty("published_iteration")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "PUBLISHED_ITERATION")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public RatingModel getPublishedIteration() {
        return publishedIteration;
    }

    @JsonProperty("published_iteration")
    public void setPublishedIteration(RatingModel publishedIteration) {
        this.publishedIteration = publishedIteration;
    }

    @JsonProperty("scoring_iteration")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "SCORING_ITERATION")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public RatingModel getScoringIteration() {
        return scoringIteration;
    }

    @JsonProperty("scoring_iteration")
    public void setScoringIteration(RatingModel scoringIteration) {
        this.scoringIteration = scoringIteration;
    }

    @JsonProperty("latest_iteration")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "LATEST_ITERATION")
    @OnDelete(action = OnDeleteAction.CASCADE)
    public RatingModel getLatestIteration() {
        return latestIteration;
    }

    @JsonProperty("latest_iteration")
    public void setLatestIteration(RatingModel latestIteration) {
        this.latestIteration = latestIteration;
    }

    @JsonIgnore
    @Lob
    @Column(name = "ADVANCED_RATING_CONFIG")
    public String getAdvancedRatingConfigStr() {
        return JsonUtils.serialize(advancedRatingConfig);
    }

    public void setAdvancedRatingConfigStr(String advancedRatingConfigStr) {
        AdvancedRatingConfig advancedRatingConfig = null;
        if (advancedRatingConfigStr != null) {
            advancedRatingConfig = JsonUtils.deserialize(advancedRatingConfigStr, AdvancedRatingConfig.class);
        }
        this.advancedRatingConfig = advancedRatingConfig;
    }

    @Transient
    @JsonProperty("advancedRatingConfig")
    public AdvancedRatingConfig getAdvancedRatingConfig() {
        return advancedRatingConfig;
    }

    public void setAdvancedRatingConfig(AdvancedRatingConfig advancedRatingConfig) {
        this.advancedRatingConfig = advancedRatingConfig;
    }

    @JsonProperty("bucketMetadata")
    @Transient
    public List<BucketMetadata> getBucketMetadata() {
        return bucketMetadata;
    }

    @JsonProperty("bucketMetadata")
    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String generateDefaultName() {
        String datePart = RatingEngine.DATE_FORMAT.format(new Date());
        String defaultName = String.format(DEFAULT_NAME_PATTERN, datePart);
        try {
            switch (getType()) {
            case RULE_BASED:
                defaultName = String.format(RULES_BASED_NAME_PATTERN, getSegment().getDisplayName(), datePart);
                break;
            case CUSTOM_EVENT:
                defaultName = String.format(CUSTOM_EVENT_NAME_PATTERN, datePart);
                break;
            case CROSS_SELL:
                if (getAdvancedRatingConfig() != null) {
                    defaultName = String.format(CROSS_SELL_NAME_PATTERN, //
                            ((CrossSellRatingConfig) getAdvancedRatingConfig())
                                    .getModelingStrategy() == ModelingStrategy.CROSS_SELL_FIRST_PURCHASE ? "First"
                                            : "Repeat",
                            datePart);
                }
                break;
            case PROSPECTING:
            default:
            }
        } catch (Exception e) {
            log.error(new LedpException(LedpCode.LEDP_40021, e, new String[] { defaultName }).getMessage(), e);
        }
        return defaultName;
    }

    public enum ScoreType {
        Rating, Probability, Score, ExpectedRevenue, PredictedRevenue
    }
}
