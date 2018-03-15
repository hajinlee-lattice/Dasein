package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FailingStep {

    @JsonProperty("seq")
    private Integer seq;

    @JsonProperty("name")
    private String name;

    @JsonProperty("occurrence")
    private Integer occurrence;

    @JsonProperty("failure")
    private InjectableFailure failure;

    public Integer getSeq() {
        return seq;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(Integer occurrence) {
        this.occurrence = occurrence;
    }

    public InjectableFailure getFailure() {
        return failure;
    }

    public void setFailure(InjectableFailure failure) {
        this.failure = failure;
    }
}
