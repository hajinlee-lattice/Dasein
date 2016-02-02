package com.latticeengines.domain.exposed.propdata.manage;

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
    private String errorMessage;

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
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
