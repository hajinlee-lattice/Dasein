package com.latticeengines.domain.exposed.cdl.activity;

import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Append;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.persistence.Basic;
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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "ATLAS_STREAM", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }), //
        @UniqueConstraint(columnNames = { "STREAM_ID", "FK_TENANT_ID" }) })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class AtlasStream implements HasPid, Serializable, HasAuditingFields {

    private static final long serialVersionUID = 7473595458075796126L;
    private static final DataFeedTask.IngestionBehavior DEFAULT_INGESTION_BEHAVIOR = Append;

    private static final String STREAM_ID_PREFIX = "as_";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("stream_id")
    @Column(name = "STREAM_ID", nullable = false)
    private String streamId;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("tenant")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "`FK_TASK_ID`", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataFeedTask dataFeedTask;

    @JsonProperty("match_entities")
    @Type(type = "json")
    @Column(name = "MATCH_ENTITIES", columnDefinition = "'JSON'", nullable = false)
    private List<String> matchEntities;

    @JsonProperty("aggr_entities")
    @Type(type = "json")
    @Column(name = "AGGR_ENTITIES", columnDefinition = "'JSON'", nullable = false)
    private List<String> aggrEntities;

    @JsonProperty("date_attribute")
    @Column(name = "DATE_ATTRIBUTE", length = 50, nullable = false)
    private String dateAttribute;

    @JsonProperty("periods")
    @Type(type = "json")
    @Column(name = "PERIODS", columnDefinition = "'JSON'", nullable = false)
    private List<String> periods;

    // if not provided, never delete
    @JsonProperty("retention_days")
    @Column(name = "RETENTION_DAYS")
    private Integer retentionDays;

    @JsonProperty("dimensions")
    @OneToMany(fetch = FetchType.LAZY, mappedBy = "stream")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<StreamDimension> dimensions;

    // configuration to derive attributes for stream
    @JsonProperty("attribute_derivers")
    @Type(type = "json")
    @Column(name = "ATTRIBUTE_DERIVER", columnDefinition = "'JSON'")
    private List<StreamAttributeDeriver> attributeDerivers;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("created")
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("updated")
    private Date updated;

    @JsonProperty("reducer")
    @Type(type = "json")
    @Column(name = "REDUCER", columnDefinition = "'JSON'")
    private ActivityRowReducer reducer;

    @JsonProperty("count")
    @Transient
    private Long count;

    @Column(name="STREAM_TYPE")
    @JsonProperty("stream_type")
    @Enumerated(EnumType.STRING)
    private StreamType streamType;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public DataFeedTask getDataFeedTask() {
        return dataFeedTask;
    }

    public void setDataFeedTask(DataFeedTask dataFeedTask) {
        this.dataFeedTask = dataFeedTask;
    }

    @JsonProperty("task_ingestion_behavior")
    public void setDataFeedTaskIngestionBehavior(DataFeedTask.IngestionBehavior behavior) {
        instantiateTaskIfNull();
        dataFeedTask.setIngestionBehavior(behavior);
    }

    @JsonProperty("task_ingestion_behavior")
    public DataFeedTask.IngestionBehavior getDataFeedTaskIngestionBehavior() {
        if (dataFeedTask == null || dataFeedTask.getIngestionBehavior() == null) {
            return DEFAULT_INGESTION_BEHAVIOR;
        }
        return dataFeedTask.getIngestionBehavior();
    }

    @JsonProperty("task_unique_id")
    public void setDataFeedTaskUniqueId(String uniqueId) {
        instantiateTaskIfNull();
        dataFeedTask.setUniqueId(uniqueId);
    }

    @JsonProperty("task_unique_id")
    public String getDataFeedTaskUniqueId() {
        return dataFeedTask == null ? null : dataFeedTask.getUniqueId();
    }

    public List<String> getMatchEntities() {
        return matchEntities;
    }

    public void setMatchEntities(List<String> matchEntities) {
        this.matchEntities = matchEntities;
    }

    public void setAggrEntities(List<String> aggrEntities) {
        this.aggrEntities = aggrEntities;
    }

    public List<String> getAggrEntities() {
        return aggrEntities;
    }

    public String getDateAttribute() {
        return dateAttribute;
    }

    public void setDateAttribute(String dateAttribute) {
        this.dateAttribute = dateAttribute;
    }

    public List<String> getPeriods() {
        return periods;
    }

    public void setPeriods(List<String> periods) {
        this.periods = periods;
    }

    public Integer getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(Integer retentionDays) {
        this.retentionDays = retentionDays;
    }

    public void setDimensions(List<StreamDimension> dimensions) {
        this.dimensions = dimensions;
    }

    public List<StreamDimension> getDimensions() {
        return dimensions;
    }

    public List<StreamAttributeDeriver> getAttributeDerivers() {
        return attributeDerivers;
    }

    public void setAttributeDerivers(List<StreamAttributeDeriver> attributeDerivers) {
        this.attributeDerivers = attributeDerivers;
    }

    public ActivityRowReducer getReducer() {
        return reducer;
    }

    public void setReducer(ActivityRowReducer reducer) {
        this.reducer = reducer;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    private void instantiateTaskIfNull() {
        if (dataFeedTask == null) {
            // dummy object
            dataFeedTask = new DataFeedTask();
        }
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

    public static String generateId() {
        String uuid;
        do {
            // try until uuid does not start with catalog prefix
            uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        } while (uuid.startsWith(STREAM_ID_PREFIX));
        return STREAM_ID_PREFIX + uuid;
    }

    public StreamType getStreamType() {
        return streamType;
    }

    public void setStreamType(StreamType streamType) {
        this.streamType = streamType;
    }

    public static final class Builder {
        private AtlasStream atlasStream;

        public Builder() {
            atlasStream = new AtlasStream();
        }

        public Builder withTenant(Tenant tenant) {
            atlasStream.setTenant(tenant);
            return this;
        }

        public Builder withDataFeedTask(DataFeedTask dataFeedTask) {
            atlasStream.setDataFeedTask(dataFeedTask);
            return this;
        }

        public Builder withName(String name) {
            atlasStream.setName(name);
            return this;
        }

        public Builder withMatchEntities(List<String> matchEntities) {
            atlasStream.setMatchEntities(matchEntities);
            return this;
        }

        public Builder withAggrEntities(List<String> aggrEntities) {
            atlasStream.setAggrEntities(aggrEntities);
            return this;
        }

        public Builder withDateAttribute(String dateAttribute) {
            atlasStream.setDateAttribute(dateAttribute);
            return this;
        }

        public Builder withPeriods(List<String> periods) {
            atlasStream.setPeriods(periods);
            return this;
        }

        public Builder withRetentionDays(Integer retentionDays) {
            atlasStream.setRetentionDays(retentionDays);
            return this;
        }

        public Builder withReducer(ActivityRowReducer reducer) {
            atlasStream.setReducer(reducer);
            return this;
        }

        public Builder withStreamType(StreamType streamType) {
            atlasStream.setStreamType(streamType);
            return this;
        }

        public Builder withAttributeDerivers(List<StreamAttributeDeriver> derivers) {
            atlasStream.setAttributeDerivers(derivers);
            return this;
        }

        public AtlasStream build() {
            return atlasStream;
        }
    }

    public enum StreamType {
        WebVisit, Opportunity, MarketingActivity, DnbIntentData
    }
}
