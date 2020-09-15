package com.latticeengines.domain.exposed.dcp;

import java.util.Date;
import java.util.List;

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

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_ENRICHMENT_LAYOUT")
public class EnrichmentLayout implements HasPid, HasTenant, SoftDeletable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "LAYOUT_ID", nullable = false)
    @JsonProperty("layout_id")
    private String layoutId;

    @Enumerated(EnumType.STRING)
    @Column(name = "domain")
    @JsonProperty("domain")
    private Domain domain;

    @Enumerated(EnumType.STRING)
    @Column(name = "RECORD_TYPE")
    @JsonProperty("record_type")
    private RecordType recordType;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created = new Date();

    @Column(name = "CREATED_BY", nullable = false)
    @JsonProperty("created_by")
    private String createdBy;

    @Column(name = "DELETED", nullable = false)
    @JsonProperty("archived")
    private Boolean deleted = Boolean.FALSE;

    @Column(name = "TEAM_ID")
    @JsonProperty("teamId")
    private String teamId;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated = new Date();

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @Column(name = "ELEMENTS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> elements;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(String layoutId) {
        this.layoutId = layoutId;
    }

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public List<String> getElements() {
        return elements;
    }

    public void setElements(List<String> elements) {
        this.elements = elements;
    }

    // TODO: change to actual types
    public enum RecordType { // These are some made up types
        RecType0, RecType1, RecType2, RecType3
    }

    // TODO: change to actual domain names
    public enum Domain {
        d0, d1, d2
    }
}
