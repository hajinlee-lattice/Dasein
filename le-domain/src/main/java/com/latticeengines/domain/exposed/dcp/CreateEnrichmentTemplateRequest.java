package com.latticeengines.domain.exposed.dcp;

public class CreateEnrichmentTemplateRequest {

    private String layoutId;

    private String templateName;

    public CreateEnrichmentTemplateRequest() {
    }

    public CreateEnrichmentTemplateRequest(String layoutId, String templateName) {
        this.layoutId = layoutId;
        this.templateName = templateName;
    }

    public String getLayoutId() {
        return layoutId;
    }

    public String getTemplateName() {
        return templateName;
    }
}
