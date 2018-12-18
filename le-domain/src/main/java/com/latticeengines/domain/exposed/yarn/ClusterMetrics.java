package com.latticeengines.domain.exposed.yarn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClusterMetrics {

    @JsonProperty
    public Integer appsPending;
    @JsonProperty
    public Integer appsRunning;

    @JsonProperty
    public Long reservedMB;
    @JsonProperty
    public Long availableMB;
    @JsonProperty
    public Long allocatedMB;
    @JsonProperty
    public Long totalMB;

    @JsonProperty
    public Integer reservedVirtualCores;
    @JsonProperty
    public Integer availableVirtualCores;
    @JsonProperty
    public Integer allocatedVirtualCores;
    @JsonProperty
    public Integer totalVirtualCores;

}
