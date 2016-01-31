package com.latticeengines.domain.exposed.propdata.manage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchOutput {

    List<OutputRecord> result;
    List<ColumnMetadata> metadata;
    MatchStatistics statistics;

    @JsonProperty("Result")
    public List<OutputRecord> getResult() {
        return result;
    }

    @JsonProperty("Result")
    public void setResult(List<OutputRecord> result) {
        this.result = result;
    }

    @JsonProperty("Metadata")
    public List<ColumnMetadata> getMetadata() {
        return metadata;
    }

    @JsonProperty("Metadata")
    public void setMetadata(List<ColumnMetadata> metadata) {
        this.metadata = metadata;
    }

    @JsonProperty("Statistics")
    public MatchStatistics getStatistics() {
        return statistics;
    }

    @JsonProperty("Statistics")
    public void setStatistics(MatchStatistics statistics) {
        this.statistics = statistics;
    }

}
