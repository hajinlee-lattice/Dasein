package com.latticeengines.domain.exposed.pls;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Front-end inputs for a modeling job.
 */
public class ModelingParameters {
    @JsonProperty
    private String filename;

    @JsonProperty
    private String name;

    @JsonProperty
    private String description;

    /**
     * In leiu of filename.
     */
    @JsonProperty
    private String eventTableName;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }
}
