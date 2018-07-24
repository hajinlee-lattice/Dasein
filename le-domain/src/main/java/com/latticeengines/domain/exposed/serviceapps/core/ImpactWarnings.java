package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ImpactWarnings {

    @JsonProperty("warnings")
    private Map<Type, List<String>> warnings = new HashMap<>();

    public Map<Type, List<String>> getWarnings() {
        return warnings;
    }

    public void setWarnings(Map<Type, List<String>> warnings) {
        this.warnings = warnings;
    }

    public enum Type {
        // dependency check warning types
        IMPACTED_SEGMENTS, //
        IMPACTED_RATING_ENGINES, //
        IMPACTED_RATING_MODELS, //
        IMPACTED_PLAYS, //
        IMPACTED_COMPANY_PROFILES, //
        // usage enabled warning types
        USAGE_ENABLED;
    }
}
