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
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "PLAY_LAUNCH")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PlayLaunch implements HasPid, HasId<String>, HasTenantId, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("launchId")
    @Index(name = "PLAY_LAUNCH_ID")
    @Column(name = "LAUNCH_ID", unique = true, nullable = false)
    private String launchId;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", nullable = true)
    private String description;

    @JsonProperty("created")
    @Index(name = "PLAY_LAUNCH_CREATED")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Index(name = "PLAY_LAUNCH_LAST_UPDATED")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("launchState")
    @Index(name = "PLAY_LAUNCH_STATE")
    @Column(name = "STATE", nullable = false)
    @Enumerated(EnumType.STRING)
    private LaunchState launchState;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PLAY_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Play play;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("applicationId")
    @Column(name = "APPLICATION_ID", nullable = true)
    private String applicationId;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonIgnore
    @Column(name = "TABLE_NAME", nullable = true)
    private String tableName;

    @JsonProperty("launchCompletionPercent")
    @Column(name = "LAUNCH_COMPLETION_PERCENT")
    private double launchCompletionPercent;

    @JsonProperty("contactsLaunched")
    @Column(name = "CONTACTS_LAUNCHED")
    private Long contactsLaunched;

    @JsonProperty("accountsLaunched")
    @Column(name = "ACCOUNTS_LAUNCHED")
    private Long accountsLaunched;

    @JsonProperty("accountsSuppressed")
    @Column(name = "ACCOUNTS_SUPPRESSED")
    private Long accountsSuppressed;

    @JsonProperty("accountsErrored")
    @Column(name = "ACCOUNTS_ERRORED")
    private Long accountsErrored;

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

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
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

    public double getLaunchCompletionPercent() {
        return launchCompletionPercent;
    }

    public void setLaunchCompletionPercent(double launchCompletionPercent) {
        this.launchCompletionPercent = launchCompletionPercent;
    }


    public Long getAccountsLaunched() {
        return this.accountsLaunched;
    }

    public void setAccountsLaunched(Long accountsLaunched) {
        this.accountsLaunched = accountsLaunched;
    }

    public Long getContactsLaunched() {
        return this.contactsLaunched;
    }

    public void setContactsLaunched(Long contactsLaunched) {
        this.contactsLaunched = contactsLaunched;
    }

    public Long getAccountsSuppressed() {
        return accountsSuppressed;
    }

    public void setAccountsSuppressed(Long accountsSuppressed) {
        this.accountsSuppressed = accountsSuppressed;
    }

    public Long getAccountsErrored() {
        return accountsErrored;
    }

    public void setAccountsErrored(Long accountsErrored) {
        this.accountsErrored = accountsErrored;
    }
}
