package com.latticeengines.domain.exposed.cdl.activity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * For dimensions which needs regex info to parse
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionCalculatorRegexMode extends DimensionCalculator {

    // attribute in stream or catalog which contains regex info
    @JsonProperty("pattern_attribute")
    private String patternAttribute;

    // whether the attribute which contains regex info is from stream of catalog
    @JsonProperty("pattern_from_catalog")
    private boolean patternFromCatalog;

    public String getPatternAttribute() {
        return patternAttribute;
    }

    public void setPatternAttribute(String patternAttribute) {
        this.patternAttribute = patternAttribute;
    }

    public boolean isPatternFromCatalog() {
        return patternFromCatalog;
    }

    public void setPatternFromCatalog(boolean patternFromCatalog) {
        this.patternFromCatalog = patternFromCatalog;
    }
}
