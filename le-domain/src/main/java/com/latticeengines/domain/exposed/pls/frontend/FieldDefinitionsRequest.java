package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinitionsRequest {

    // All Field Definition Requests in version 2.0 need to have a TemplateState object to identify basic state
    // information for the request.
    @JsonProperty
    protected TemplateState templateState;

    public TemplateState getTemplateState() {
        return templateState;
    }

    public void setTemplateState(TemplateState templateState) {
        this.templateState = templateState;
    }

    @Override
    public String toString() {
        return "templateState:\n" + templateState.toString();
    }
}
