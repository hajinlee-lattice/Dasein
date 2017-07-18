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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "PLAY_LAUNCH")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PlayLaunch implements HasPid, HasId<String>, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "PLAY_LAUNCH_ID")
    @Column(name = "LAUNCH_ID", unique = true, nullable = false)
    @JsonProperty("launch_id")
    private String launchId;

    @Column(name = "DESCRIPTION", nullable = true)
    @JsonProperty("description")
    private String description;

    @Index(name = "PLAY_LAUNCH_CREATED_TIME")
    @Column(name = "CREATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("createdTimestamp")
    private Date createdTimestamp;

    @Index(name = "PLAY_LAUNCH_LAST_UPD_TIME")
    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("lastUpdatedTimestamp")
    private Date lastUpdatedTimestamp;

    @Index(name = "PLAY_LAUNCH_STATE")
    @Column(name = "STATE", nullable = false)
    @Enumerated(EnumType.STRING)
    @JsonProperty("launch_state")
    private LaunchState launchState;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PLAY_ID", nullable = false)
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Play play;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("ApplicationId")
    @Column(name = "APPLICATION_ID", nullable = true)
    private String applicationId;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Column(name = "TABLE_NAME", nullable = true)
    @JsonIgnore
    private String tableName;

    @Column(name = "CONTACTS_NUM")
    @JsonProperty("contacts_num")
    private Long contactsNum;

    @Column(name = "ACCOUNTS_NUM")
    @JsonProperty("accounts_num")
    private Long accountsNum;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String id) {
        this.launchId = id;
    }

    @Override
    public String getId() {
        return launchId;
    }

    @Override
    public void setId(String launchId) {
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

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationId() {
        return applicationId;
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

    public Long getAccountsNum() {
        return this.accountsNum;
    }

    public void setAccountsNum(Long accountsNum) {
        this.accountsNum = accountsNum;
    }

    public Long getContactsNum() {
        return this.contactsNum;
    }

    public void setContactsNum(Long contactsNum) {
        this.contactsNum = contactsNum;
    }
}
