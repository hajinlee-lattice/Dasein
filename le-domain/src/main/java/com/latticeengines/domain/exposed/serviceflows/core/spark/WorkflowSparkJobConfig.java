package com.latticeengines.domain.exposed.serviceflows.core.spark;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

import reactor.core.publisher.Flux;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Name")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = ParseMatchResultJobConfig.class, name = ParseMatchResultJobConfig.NAME), //
})
public abstract class WorkflowSparkJobConfig extends SparkJobConfig {

}
