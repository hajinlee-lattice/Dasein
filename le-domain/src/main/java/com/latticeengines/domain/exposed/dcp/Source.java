package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Source {

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("source_display_name")
    private String sourceDisplayName;

    @JsonProperty("project")
    private Project project;

    private String relativePath;

}
