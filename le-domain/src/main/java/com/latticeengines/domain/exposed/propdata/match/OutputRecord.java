package com.latticeengines.domain.exposed.propdata.match;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutputRecord {

    private Integer rowNumber;
    private Boolean matched;
    private List<Object> input;
    private String matchedDomain;
    private List<Object> output;
    private List<String> errorMessages;

    @JsonProperty("RowNumber")
    public Integer getRowNumber() {
        return rowNumber;
    }

    @JsonProperty("RowNumber")
    public void setRowNumber(Integer rowNumber) {
        this.rowNumber = rowNumber;
    }

    @JsonProperty("IsMatched")
    public Boolean isMatched() {
        return matched;
    }

    @JsonProperty("IsMatched")
    public void setMatched(Boolean matched) {
        this.matched = matched;
    }

    @JsonProperty("Input")
    public List<Object> getInput() {
        return input;
    }

    @JsonProperty("Input")
    public void setInput(List<Object> input) {
        this.input = input;
    }

    @JsonProperty("MatchedDomain")
    public String getMatchedDomain() {
        return matchedDomain;
    }

    @JsonProperty("MatchedDomain")
    public void setMatchedDomain(String matchedDomain) {
        this.matchedDomain = matchedDomain;
    }

    @JsonProperty("Output")
    public List<Object> getOutput() {
        return output;
    }

    @JsonProperty("Output")
    public void setOutput(List<Object> output) {
        this.output = output;
    }

    @JsonProperty("ErrorMessage")
    public List<String> getErrorMessages() {
        return errorMessages;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessages(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public void addErrorMessage(String errorMessage) {
        List<String> msgs;
        if (this.errorMessages == null) {
            msgs = new ArrayList<>();
        } else {
            msgs = new ArrayList<>(this.errorMessages);
        }
        msgs.add(errorMessage);
        this.errorMessages = msgs;
    }
}
