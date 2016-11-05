package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OutputRecord {

    @JsonProperty("RowNumber")
    private Integer rowNumber;

    @JsonProperty("IsMatched")
    private Boolean matched;

    @JsonProperty("Input")
    private List<Object> input;

    @JsonProperty("PreMatchDomain")
    private String preMatchDomain;

    @JsonProperty("PreMatchNameLocation")
    private NameLocation preMatchNameLocation;

    @JsonProperty("PreMatchDUNS")
    private String preMatchDuns;

    @JsonProperty("PreMatchEmail")
    private String preMatchEmail;

    @JsonProperty("Output")
    private List<Object> output;

    @JsonProperty("matchLogs")
    private List<String> matchLogs;

    public Integer getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(Integer rowNumber) {
        this.rowNumber = rowNumber;
    }

    public Boolean isMatched() {
        return matched;
    }

    public void setMatched(Boolean matched) {
        this.matched = matched;
    }

    public List<Object> getInput() {
        return input;
    }

    public void setInput(List<Object> input) {
        this.input = input;
    }

    public String getPreMatchDomain() {
        return preMatchDomain;
    }

    public void setPreMatchDomain(String preMatchDomain) {
        this.preMatchDomain = preMatchDomain;
    }

    public NameLocation getPreMatchNameLocation() {
        return preMatchNameLocation;
    }

    public void setPreMatchNameLocation(NameLocation preMatchNameLocation) {
        this.preMatchNameLocation = preMatchNameLocation;
    }

    public String getPreMatchDuns() {
        return preMatchDuns;
    }

    public void setPreMatchDuns(String preMatchDuns) {
        this.preMatchDuns = preMatchDuns;
    }

    public String getPreMatchEmail() {
        return preMatchEmail;
    }

    public void setPreMatchEmail(String preMatchEmail) {
        this.preMatchEmail = preMatchEmail;
    }

    public List<Object> getOutput() {
        return output;
    }

    public void setOutput(List<Object> output) {
        this.output = output;
    }

    public List<String> getMatchLogs() {
        return matchLogs;
    }

    public void setMatchLogs(List<String> matchLogs) {
        this.matchLogs = matchLogs;
    }

    public void log(String log) {
        if (this.matchLogs == null) {
            matchLogs = new ArrayList<>();
        }
        matchLogs.add(log);
    }

}
