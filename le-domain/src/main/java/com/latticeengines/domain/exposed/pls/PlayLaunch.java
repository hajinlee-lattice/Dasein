package com.latticeengines.domain.exposed.pls;

import java.util.Date;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "PLAY_LAUNCH")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PlayLaunch implements HasPid, HasId<String>, HasName, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "LAUNCH_ID", nullable = false, unique = true)
    @JsonProperty("launch_id")
    private String id;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @Column(name = "TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("timestamp")
    private Date timestamp;

    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("lastUpdatedTimestamp")
    private Date lastUpdatedTimestamp;

    @Column(name = "STATE", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    @JsonProperty("launch_state")
    private LaunchState launchState;

    @ManyToOne
    @JoinColumn(name = "FK_PLAY_ID", nullable = false)
    @JsonIgnore
    private Play play;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Column(name = "TABLE_NAME", nullable = true)
    @JsonIgnore
    private String tableName;

    @Transient
    @JsonProperty("table")
    private Table table;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public LaunchState getLaunchState() {
        return launchState;
    }

    public void setLaunchState(LaunchState launchState) {
        this.launchState = launchState;
    }

    public Play getPlay() {
        return play;
    }

    public void setPlay(Play play) {
        this.play = play;
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
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    @JsonIgnore
    public Long getTenantId() {
        return this.tenantId;
    }
}
