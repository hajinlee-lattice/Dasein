package com.latticeengines.domain.exposed.yarn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClusterMetrics {

    public Integer appsPending;
    public Integer appsRunning;

    public Integer reservedMB;
    public Integer availableMB;
    public Integer allocatedMB;
    public Integer totalMB;

    public Integer reservedVirtualCores;
    public Integer availableVirtualCores;
    public Integer allocatedVirtualCores;
    public Integer totalVirtualCores;

}
