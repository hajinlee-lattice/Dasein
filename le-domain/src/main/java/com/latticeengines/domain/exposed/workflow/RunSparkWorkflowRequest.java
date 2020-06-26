package com.latticeengines.domain.exposed.workflow;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class RunSparkWorkflowRequest {

    @NotNull
    @JsonProperty("sparkJobClz")
    private String sparkJobClz;

    @NotNull
    @JsonProperty("sparkJobConfig")
    private SparkJobConfig sparkJobConfig;

    @JsonProperty("partitionMultiplier")
    private Integer partitionMultiplier;

    // keep the random workspace afterwards
    @JsonProperty("keepWorkspace")
    private Boolean keepWorkspace;

    public String getSparkJobClz() {
        return sparkJobClz;
    }

    public void setSparkJobClz(String sparkJobClz) {
        this.sparkJobClz = sparkJobClz;
    }

    public SparkJobConfig getSparkJobConfig() {
        return sparkJobConfig;
    }

    public void setSparkJobConfig(SparkJobConfig sparkJobConfig) {
        this.sparkJobConfig = sparkJobConfig;
    }

    public Integer getPartitionMultiplier() {
        return partitionMultiplier;
    }

    public void setPartitionMultiplier(Integer partitionMultiplier) {
        this.partitionMultiplier = partitionMultiplier;
    }

    public Boolean getKeepWorkspace() {
        return keepWorkspace;
    }

    public void setKeepWorkspace(Boolean keepWorkspace) {
        this.keepWorkspace = keepWorkspace;
    }
}
