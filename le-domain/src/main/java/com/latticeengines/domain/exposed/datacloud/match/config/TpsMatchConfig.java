package com.latticeengines.domain.exposed.datacloud.match.config;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.contactmaster.LiveRampDestination;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class TpsMatchConfig {

    @JsonProperty("jobFunctions")
    private List<String> jobFunctions;

    @JsonProperty("jobLevels")
    private List<String> jobLevels;

    @JsonProperty("destination")
    private LiveRampDestination destination;

    public List<String> getJobFunctions() {
        return jobFunctions;
    }

    public void setJobFunctions(List<String> jobFunctions) {
        this.jobFunctions = jobFunctions;
    }

    public List<String> getJobLevels() {
        return jobLevels;
    }

    public void setJobLevels(List<String> jobLevels) {
        this.jobLevels = jobLevels;
    }

    public LiveRampDestination getDestination() {
        return destination;
    }

    public void setDestination(LiveRampDestination destination) {
        this.destination = destination;
    }
}
