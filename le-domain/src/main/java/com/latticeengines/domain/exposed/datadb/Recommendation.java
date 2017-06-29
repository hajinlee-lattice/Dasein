package com.latticeengines.domain.exposed.datadb;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity 
@javax.persistence.Table(name = "Recommendation")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Recommendation implements HasPid, HasId<String>, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "REC_EXTERNAL_ID")
    @Column(name = "EXTERNAL_ID", nullable = false)
    @JsonProperty("external_id")
    private String recommendationId;

    @Index(name = "REC_PLAY_ID")
    @Column(name = "PLAY_ID", nullable = false)
    @JsonProperty("play_id")
    private String playId;

    @Index(name = "REC_LAUNCH_ID")
    @Column(name = "LAUNCH_ID", nullable = false)
    @JsonProperty("launch_id")
    private String launchId;

    @Column(name = "DESCRIPTION", nullable = true)
    @JsonProperty("description")
    private String description;

    @Index(name = "REC_LAUNCH_CREATED_TIME")
    @Column(name = "CREATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("createdTimestamp")
    private Date createdTimestamp;

    @Index(name = "REC_LAUNCH_LAST_UPD_TIME")
    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("lastUpdatedTimestamp")
    private Date lastUpdatedTimestamp;

    @Index(name = "REC_TENANT_ID")
    @Column(name = "TENANT_ID", nullable = false)
    @JsonProperty("tenant_id")
    private Long tenantId;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getRecommendationId() {
        return recommendationId;
    }

    public void setRecommendationId(String recommendationId) {
        this.recommendationId = recommendationId;
    }

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Date createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public Date getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String getId() {
        return recommendationId;
    }

    @Override
    public void setId(String id) {
        this.recommendationId = id;
    }
}
