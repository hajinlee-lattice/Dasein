package com.latticeengines.domain.exposed.metadata.datafeed;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.TemplateValidator;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataFeedTaskConfig {

    @JsonProperty("limit_per_import")
    private Long limitPerImport;

    @JsonProperty("validators")
    private ValidatorList templateValidators;

    public Long getLimitPerImport() {
        return limitPerImport;
    }

    public void setLimitPerImport(Long limitPerImport) {
        this.limitPerImport = limitPerImport;
    }

    public ValidatorList getTemplateValidators() {
        return templateValidators;
    }

    public void setTemplateValidators(ValidatorList templateValidators) {
        this.templateValidators = templateValidators;
    }

    @JsonIgnore
    public void addTemplateValidator(TemplateValidator templateValidator) {
        if (templateValidators == null) {
            templateValidators = new ValidatorList();
        }
        templateValidators.add(templateValidator);
    }

    // for jackson serialization
    public static class ValidatorList extends ArrayList<TemplateValidator> {

    }
}
