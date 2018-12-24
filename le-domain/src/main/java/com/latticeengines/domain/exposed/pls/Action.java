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
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ACTION")
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class Action implements HasPid, HasTenant, HasAuditingFields {

    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private ActionType type;

    @JsonProperty("ownerId")
    @Column(name = "OWNER_ID")
    private Long ownerId;

    @JsonProperty("trackingPid")
    @Column(name = "TRACKING_PID")
    private Long trackingPid;

    @JsonProperty("actionInitiator")
    @Column(name = "ACTION_INITIATOR")
    private String actionInitiator;

    @JsonIgnore
    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION")
    @Type(type = "text")
    private String description;

    @JsonProperty("actionConfiguration")
    @Column(name = "ACTION_CONFIGURATION", columnDefinition = "'JSON'")
    @Type(type = "json")
    private ActionConfiguration actionConfiguration;

    @JsonProperty("actionStatus")
    @Column(name = "ACTION_STATUS", length = 20)
    @Enumerated(EnumType.STRING)
    private ActionStatus actionStatus = ActionStatus.ACTIVE;

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public ActionType getType() {
        return this.type;
    }

    public void setType(ActionType type) {
        this.type = type;
    }

    public Long getOwnerId() {
        return this.ownerId;
    }

    public void setOwnerId(Long id) {
        this.ownerId = id;
    }

    public Long getTrackingPid() {
        return this.trackingPid;
    }

    public void setTrackingPid(Long trackingPid) {
        this.trackingPid = trackingPid;
    }

    public String getActionInitiator() {
        return this.actionInitiator;
    }

    public void setActionInitiator(String actionInitiator) {
        this.actionInitiator = actionInitiator;
    }

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    public Date getUpdated() {
        return this.updated;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    public ActionConfiguration getActionConfiguration() {
        return this.actionConfiguration;
    }

    public void setActionConfiguration(ActionConfiguration actionConfiguration) {
        this.actionConfiguration = actionConfiguration;
    }

    public ActionStatus getActionStatus() {
        return actionStatus;
    }

    public void setActionStatus(ActionStatus actionStatus) {
        this.actionStatus = actionStatus;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
