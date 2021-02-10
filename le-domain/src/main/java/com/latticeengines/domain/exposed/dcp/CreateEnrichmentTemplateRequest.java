package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateEnrichmentTemplateRequest {

    @JsonProperty
    private String layoutId;

    @JsonProperty
    private String templateName;

    @JsonProperty
    private String createdBy;

    public CreateEnrichmentTemplateRequest() {}

    public CreateEnrichmentTemplateRequest(String layoutId, String templateName, String createdBy) {
        this.layoutId = layoutId;
        this.templateName = templateName;
        this.createdBy = createdBy;
    }

    public String getLayoutId() {
        return layoutId;
    }

    public String getTemplateName() {
        return templateName;
    }

    public String getCreatedBy() {
        return createdBy;
    }
}
