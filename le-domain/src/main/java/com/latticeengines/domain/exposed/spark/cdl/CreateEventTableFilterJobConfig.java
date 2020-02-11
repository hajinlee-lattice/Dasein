package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CreateEventTableFilterJobConfig extends SparkJobConfig {

    public static final String NAME = "createEventTableFilterJobConfig";

    @JsonProperty("event_column")
    private String eventColumn;

    public CreateEventTableFilterJobConfig() {
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
