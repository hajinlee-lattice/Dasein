package com.latticeengines.domain.exposed.dcp;

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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "DCP_UPLOAD", indexes = { @Index(name = "IX_SOURCE_ID", columnList = "SOURCE_ID") })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class Upload implements HasPid, HasTenant, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "UPLOAD_ID", nullable = false)
    private String uploadId;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "SOURCE_ID", nullable = false)
    private String sourceId;

    @Column(name = "STATUS", length = 40)
    @Enumerated(EnumType.STRING)
    private Status status;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_MATCH_RESULT")
    private Table matchResult;

    @Transient
    private String matchResultTableName;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_MATCH_CANDIDATES")
    private Table matchCandidates;

    @Transient
    private String matchCandidatesTableName;

    @Column(name = "UPLOAD_CONFIG", columnDefinition = "'JSON'", length = 8000)
    @Type(type = "json")
    private UploadConfig uploadConfig;

    @Transient
    public UploadStats statistics;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
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

    public UploadStats getStatistics() {
        return statistics;
    }

    public void setStatistics(UploadStats statistics) {
        this.statistics = statistics;
    }

    private Table getMatchResult() {
        return matchResult;
    }

    public void setMatchResult(Table matchResult) {
        this.matchResult = matchResult;
        if (matchResult != null) {
            this.matchResultTableName = matchResult.getName();
        }
    }

    public String getMatchResultTableName() {
        if (StringUtils.isBlank(matchResultTableName) && this.matchResult != null) {
            matchResultTableName = this.matchResult.getName();
        }
        return matchResultTableName;
    }

    private void setMatchResultTableName(String matchResultTableName) {
        this.matchResultTableName = matchResultTableName;
    }

    private Table getMatchCandidates() {
        return matchCandidates;
    }

    public void setMatchCandidates(Table matchCandidates) {
        this.matchCandidates = matchCandidates;
        if (matchCandidates != null) {
            this.matchCandidatesTableName = matchCandidates.getName();
        }
    }

    public String getMatchCandidatesTableName() {
        if (StringUtils.isBlank(matchCandidatesTableName) && this.matchCandidates != null) {
            matchCandidatesTableName = this.matchCandidates.getName();
        }
        return matchCandidatesTableName;
    }

    private void setMatchCandidatesTableName(String matchCandidatesTableName) {
        this.matchCandidatesTableName = matchCandidatesTableName;
    }

    // TODO: more specific status.
    public enum Status{
        NEW,
        IMPORT_STARTED,
        IMPORT_FINISHED,
        MATCH_STARTED,
        MATCH_FINISHED,
        FINISHED
    }
}
