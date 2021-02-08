package com.latticeengines.domain.exposed.dcp;

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

    public EnrichmentTemplateSummary() {
    }

    public EnrichmentTemplateSummary(EnrichmentTemplate enrichmentTemplate) {
        this.templateId = enrichmentTemplate.getTemplateId();
        this.templateName = enrichmentTemplate.getTemplateName();
        this.domain = enrichmentTemplate.getDomain();
        this.recordType = enrichmentTemplate.getRecordType();
        this.createdBy = enrichmentTemplate.getCreatedBy();
        this.elements = enrichmentTemplate.getElements();
    }
}
