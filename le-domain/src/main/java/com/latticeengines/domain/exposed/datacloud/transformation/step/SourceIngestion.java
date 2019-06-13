package com.latticeengines.domain.exposed.datacloud.transformation.step;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceIngestion {

    @JsonProperty("IngestionName")
    private String ingestionName;

    public SourceIngestion(String ingestionName) {
        this.ingestionName = ingestionName;
    }

    public String getIngestionName() {
        return ingestionName;
    }

    public void setIngestionName(String ingestionName) {
        this.ingestionName = ingestionName;
    }
}
