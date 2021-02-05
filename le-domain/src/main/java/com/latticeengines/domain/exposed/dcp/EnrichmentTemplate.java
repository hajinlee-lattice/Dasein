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
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_ENRICHMENT_TEMPLATE", uniqueConstraints = {
        @UniqueConstraint(name = "UX_LAYOUT_TEMPLATE_ID", columnNames = { "TEMPLATE_ID" }) })
public class EnrichmentTemplate implements HasPid, HasTenant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "TEMPLATE_ID", nullable = false)
    @JsonProperty("templateId")
    private String templateId;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("templateName")
    private String templateName;

    @Enumerated(EnumType.STRING)
    @Column(name = "DOMAIN", nullable = false)
    @JsonProperty("domain")
    private DataDomain domain;

    @Enumerated(EnumType.STRING)
    @Column(name = "RECORD_TYPE", nullable = false)
    @JsonProperty("recordType")
    private DataRecordType recordType;

    @Column(name = "ELEMENTS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> elements;

    @Column(name = "ARCHIVED", nullable = false)
    @JsonProperty("archived")
    private Boolean archived;

    @Column(name = "CREATE_TIME", nullable = false)
    @JsonProperty("createTime")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createTime = new Date();

    @Column(name = "UPDATE_TIME", nullable = false)
    @JsonProperty("updateTime")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated = new Date();

    @Column(name = "CREATED_BY", nullable = false)
    @JsonProperty("createdBy")
    private String createdBy;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    public EnrichmentTemplate() {
        this.templateId = NamingUtils.uuid("Template");
    }

    public EnrichmentTemplate(EnrichmentLayout enrichmentLayout) {
        this.templateId = NamingUtils.uuid("Template");
        this.domain = enrichmentLayout.getDomain();
        this.recordType = enrichmentLayout.getRecordType();
        this.elements = enrichmentLayout.getElements();
        this.createTime = enrichmentLayout.getCreated();
        this.updated = enrichmentLayout.getUpdated();
        this.createdBy = enrichmentLayout.getCreatedBy();
        this.archived = false;
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
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getTemplateName() {
        return this.templateName;
    }

    public String getTemplateId() {
        return templateId;
    }

    public DataDomain getDomain() {
        return domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public List<String> getElements() {
        return elements;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public void setElements(List<String> elements) {
        this.elements = elements;
    }

    public void setRecordType(DataRecordType recordType) {
        this.recordType = recordType;
    }

    public Boolean getArchived() {
        return archived;
    }

}
