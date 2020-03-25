package com.latticeengines.domain.exposed.dcp;

import java.util.Date;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_UPLOAD", indexes = { @Index(name = "IX_SOURCE_ID", columnList = "SOURCE_ID") })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class Upload implements HasPid, HasTenant, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "SOURCE_ID", nullable = false)
    @JsonProperty("source_id")
    private String sourceId;

    @Column(name = "STATUS")
    @JsonProperty("status")
    private Status status;

    @JsonProperty("upload_config")
    @Column(name = "UPLOAD_CONFIG", columnDefinition = "'JSON'", length = 8000)
    @Type(type = "json")
    private UploadConfig uploadConfig;

    @JsonProperty("upload_stats")
    @Column(name = "UPLOAD_STATS", columnDefinition = "'JSON'", length = 8000)
    @Type(type = "json")
    private UploadStats uploadStats;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date date) {
        this.created = date;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date date) {
        this.updated = date;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public UploadConfig getUploadConfig() {
        return uploadConfig;
    }

    public void setUploadConfig(UploadConfig uploadConfig) {
        this.uploadConfig = uploadConfig;
    }

    public UploadStats getUploadStats() {
        return uploadStats;
    }

    public void setUploadStats(UploadStats uploadStats) {
        this.uploadStats = uploadStats;
    }

    // TODO: more specific status.
    public enum Status{
        NEW,
        IMPORT_STARTED,
        IMPORT_FINISHED,
        MATCH_STARTED,
        MATCH_FINISHED
    }
}
