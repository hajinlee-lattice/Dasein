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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_ENRICHMENT_LAYOUT", uniqueConstraints = { //
        @UniqueConstraint(name = "UX_LAYOUT_LAYOUT_ID", columnNames = { "LAYOUT_ID" }), //
        @UniqueConstraint(name = "UX_LAYOUT_SOURCE_ID", columnNames = { "SOURCE_ID" }) //
})
public class EnrichmentLayout implements HasPid, HasTenant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "LAYOUT_ID", nullable = false)
    @JsonProperty("layoutId")
    private String layoutId;

    @Column(name = "SOURCE_ID", nullable = false)
    @JsonProperty("sourceId")
    private String sourceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "domain")
    @JsonProperty("domain")
    private DataDomain domain;

    @Enumerated(EnumType.STRING)
    @Column(name = "RECORD_TYPE")
    @JsonProperty("recordType")
    private DataRecordType recordType;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created = new Date();

    @Column(name = "CREATED_BY", nullable = false)
    @JsonProperty("createdBy")
    private String createdBy;

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

    @Column(name = "TEMPLATE_ID")
    @JsonProperty("templateId")
    private String templateId;

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

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public DataDomain getDomain() {
        return domain;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(DataRecordType recordType) {
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

    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }
}
