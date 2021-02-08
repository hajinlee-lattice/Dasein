package com.latticeengines.domain.exposed.dcp;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

public class EnrichmentTemplateSummary {

    @JsonProperty("templateId")
    public String templateId;

    @JsonProperty("templateName")
    public String templateName;

    @JsonProperty("domain")
    public DataDomain domain;

    @JsonProperty("recordType")
    public DataRecordType recordType;

    @JsonProperty("createdBy")
    public String createdBy;

    @JsonProperty("elements")
    public List<String> elements;

    @JsonProperty("createTime")
    private Date createTime;

    @JsonProperty("updateTime")
    private Date updateTime;

    public EnrichmentTemplateSummary() {
    }

    public EnrichmentTemplateSummary(EnrichmentTemplate enrichmentTemplate) {
        this.templateId = enrichmentTemplate.getTemplateId();
        this.templateName = enrichmentTemplate.getTemplateName();
        this.domain = enrichmentTemplate.getDomain();
        this.recordType = enrichmentTemplate.getRecordType();
        this.createdBy = enrichmentTemplate.getCreatedBy();
        this.elements = enrichmentTemplate.getElements();
        this.createTime = enrichmentTemplate.getCreateTime();
        this.updateTime = enrichmentTemplate.getUpdated();
    }

    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public List<String> getElements() {
        return elements;
    }

    public void setElements(List<String> elements) {
        this.elements = elements;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
