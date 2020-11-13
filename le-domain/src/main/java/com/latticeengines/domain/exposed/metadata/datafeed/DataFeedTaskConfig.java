package com.latticeengines.domain.exposed.metadata.datafeed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.datafeed.sanitizer.InputValueSanitizer;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.TemplateValidator;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataFeedTaskConfig {

    @JsonProperty("limit_per_import")
    private Long limitPerImport;

    @JsonProperty("validators")
    private ValidatorList templateValidators;

    @JsonProperty("sanitizers")
    private SanitizerList sanitizers;

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

    public SanitizerList getSanitizers() {
        return sanitizers;
    }

    public void setSanitizers(SanitizerList sanitizers) {
        this.sanitizers = sanitizers;
        if (this.sanitizers != null) {
            this.sanitizers.sort();
        }
    }

    @JsonIgnore
    public void addTemplateValidator(TemplateValidator templateValidator) {
        if (templateValidators == null) {
            templateValidators = new ValidatorList();
        }
        templateValidators.add(templateValidator);
    }

    @JsonIgnore
    public void addSanitizer(InputValueSanitizer sanitizer) {
        if (sanitizers == null) {
            sanitizers = new SanitizerList();
        }
        sanitizers.add(sanitizer);
    }

    // for jackson serialization
    public static class ValidatorList extends ArrayList<TemplateValidator> {

    }

    public static class SanitizerList extends ArrayList<InputValueSanitizer> {

        @Override
        public boolean add(InputValueSanitizer regulator) {
            if (regulator == null) {
                return false;
            }
            int i = 0;
            for (; i < size(); i++) {
                if (get(i).getOrder() > regulator.getOrder()) {
                    break;
                }
            }
            super.add(i, regulator);
            return true;
        }

        public void sort() {
            super.sort(Comparator.comparing(InputValueSanitizer::getOrder));
        }

        public String sanitize(String input, Attribute attribute) {
            Iterator<InputValueSanitizer> iterator = iterator();
            String result = input;
            while (iterator.hasNext()) {
                result = iterator.next().sanitize(result, attribute);
            }
            return result;
        }
    }
}
