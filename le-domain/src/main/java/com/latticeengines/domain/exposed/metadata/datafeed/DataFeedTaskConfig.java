package com.latticeengines.domain.exposed.metadata.datafeed;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataFeedTaskConfig {

    @JsonProperty("limit_per_import")
    private Long limitPerImport;

    public Long getLimitPerImport() {
        return limitPerImport;
    }

    public void setLimitPerImport(Long limitPerImport) {
        this.limitPerImport = limitPerImport;
    }
}
