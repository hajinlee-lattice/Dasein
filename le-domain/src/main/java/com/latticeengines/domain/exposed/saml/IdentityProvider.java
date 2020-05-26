package com.latticeengines.domain.exposed.saml;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;

@Entity
@Table(name = "SECURITY_IDENTITY_PROVIDER", indexes = {
        @Index(name = "IX_ENTITY_ID", columnList = "ENTITY_ID") },
        uniqueConstraints = {@UniqueConstraint(name = "UX_IDENTITY_ID", columnNames = { "TENANT_ID", "ENTITY_ID" })})

public class IdentityProvider implements HasPid, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthTenant globalAuthTenant;

    @JsonProperty("entity_id")
    @Column(name = "ENTITY_ID", nullable = false)
    private String entityId;

    @JsonProperty("config_id")
    @Transient
    private String entityIdBase64;

    @JsonProperty("metadata")
    @Column(name = "METADATA", nullable = false)
    @Type(type = "text")
    private String metadata;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    private Date updated;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    private Date created;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityIdBase64() {
        if (StringUtils.isEmpty(this.entityIdBase64) && !StringUtils.isEmpty(this.entityId)) {
            this.entityIdBase64 = Base64Utils.encodeBase64(this.entityId.getBytes());
        }
        return entityIdBase64;
    }

    public void setEntityIdBase64(String entityIdBase64) {
        this.entityIdBase64 = entityIdBase64;
        if (!StringUtils.isEmpty(this.entityIdBase64) && StringUtils.isEmpty(this.entityId)) {
            this.entityId = new String(Base64Utils.decodeBase64(this.entityIdBase64));
        }
    }

    public GlobalAuthTenant getGlobalAuthTenant() {
        return globalAuthTenant;
    }

    public void setGlobalAuthTenant(GlobalAuthTenant globalAuthTenant) {
        this.globalAuthTenant = globalAuthTenant;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
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
    public String toString() {
        // we only want to log metadata and entity_id
        Map<String, String> identityProviderForLog = new HashMap<>();
        identityProviderForLog.put("metadata", metadata);
        if (org.apache.commons.lang3.StringUtils.isNotBlank(entityId)) {
            identityProviderForLog.put("entity_id", entityId);
        }
        return JsonUtils.serialize(identityProviderForLog);
    }
}
