package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "PLAY_GROUP")
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
        @ParamDef(name = "tenantFilterId", type = "java.lang.Long") })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class PlayGroup implements HasPid, HasId<String>, HasTenantId, HasAuditingFields {
    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("id")
    @Column(name = "ID", nullable = false)
    private String id;

    @JsonProperty("displayName")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

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

    @JsonProperty("updatedBy")
    @Column(name = "UPDATED_BY", nullable = false)
    private String updatedBy;

    @JsonIgnore
    @ManyToMany(fetch = FetchType.LAZY, mappedBy = "playGroups")
    private Set<Play> plays;

    public PlayGroup() {
    }

    public PlayGroup(Tenant tenant, String displayName, String createdBy, String updatedBy) {
        this.tenant = tenant;
        this.tenantId = tenant.getPid();
        this.displayName = displayName;
        this.createdBy = createdBy;
        this.updatedBy = updatedBy;
        this.id = generateId();
    }

    public static String generateId() {
        return UUID.randomUUID().toString();
    }

    public static String getIdForBIS(String type) {
        return StringUtils.isNotEmpty(type) && type.equals("List") ? "ADefault" : type;
    }

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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @JsonIgnore
    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @JsonIgnore
    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Set<Play> getPlays() {
        return plays;
    }

    public void setPlays(Set<Play> plays) {
        this.plays = plays;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
}
