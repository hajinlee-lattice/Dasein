package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "TIME_LINE", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"TIMELINE_ID", "FK_TENANT_ID"})})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeLine implements HasPid, HasTenant, Serializable {

    private static final long serialVersionUID = 0L;

    private static final String TIMELINE_ID_PREFIX = "tl_";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "TIMELINE_ID", nullable = false)
    @JsonProperty("timeline_id")
    private String timelineId;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String Name;

    @Column(name = "ENTITY", nullable = false)
    @JsonProperty("entity")
    private String entity;

    @Column(name = "STREAM_TYPES", columnDefinition = "'JSON'")
    @JsonProperty("stream_types")
    @Type(type = "json")
    private List<AtlasStream.StreamType> streamTypes;

    @Column(name = "STREAM_IDS", columnDefinition = "'JSON'")
    @JsonProperty("stream_ids")
    @Type(type = "json")
    private List<String> streamIds;

    /**
     *streamType -> (desCol, srcColExtractor{srcCol, mappingType})
     */
    @Column(name = "EVENT_MAPPINGS", columnDefinition = "'JSON'")
    @JsonProperty("event_mappings")
    @Type(type = "json")
    private Map<String, Map<String, EventTypeExtractor>> eventMappings;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Map<String, Map<String, EventTypeExtractor>> getEventMappings() {
        return eventMappings;
    }

    public void setEventMappings(Map<String, Map<String, EventTypeExtractor>> eventMappings) {
        this.eventMappings = eventMappings;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public List<AtlasStream.StreamType> getStreamTypes() {
        return streamTypes;
    }

    public void setStreamTypes(List<AtlasStream.StreamType> streamTypes) {
        this.streamTypes = streamTypes;
    }

    public String getTimelineId() {
        return timelineId;
    }

    public void setTimelineId(String timelineId) {
        this.timelineId = timelineId;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public List<String> getStreamIds() {
        return streamIds;
    }

    public void setStreamIds(List<String> streamIds) {
        this.streamIds = streamIds;
    }

    public static String generateId() {
        String uuid;
        do {
            // try until uuid does not start with catalog prefix
            uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        } while (uuid.startsWith(TIMELINE_ID_PREFIX));
        return TIMELINE_ID_PREFIX + uuid;
    }
}
