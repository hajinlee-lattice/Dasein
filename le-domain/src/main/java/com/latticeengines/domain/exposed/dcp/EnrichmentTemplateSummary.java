package com.latticeengines.domain.exposed.dcp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

public class EnrichmentTemplateSummary {

    @JsonProperty("templateId")
    private String templateId;

    @JsonProperty("templateName")
    private String templateName;

    @JsonProperty("domain")
    private DataDomain domain;

    @JsonProperty("recordType")
    private DataRecordType recordType;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("elements")
    private List<String> elements;

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