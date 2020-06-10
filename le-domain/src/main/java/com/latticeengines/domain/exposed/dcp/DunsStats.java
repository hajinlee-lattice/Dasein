package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DunsStats {

    @JsonProperty("distinct_count")
    private Long distinctCnt;

    @JsonProperty("unique_count")
    private Long uniqueCnt;

    @JsonProperty("duplicated_count")
    private Long duplicatedCnt;

    @JsonProperty("duplicated_percentage")
    private Double dupPercent;

    @JsonProperty("unique_percentage")
    private Double uniPercent;

    public Long getDistinctCnt() {
        return distinctCnt;
    }

    public void setDistinctCnt(Long distinctCnt) {
        this.distinctCnt = distinctCnt;
    }

    public Long getUniqueCnt() {
        return uniqueCnt;
    }

    public void setUniqueCnt(Long uniqueCnt) {
        this.uniqueCnt = uniqueCnt;
    }

    public Long getDuplicatedCnt() {
        return duplicatedCnt;
    }

    public void setDuplicatedCnt(Long duplicatedCnt) {
        this.duplicatedCnt = duplicatedCnt;
    }

    public Double getDupPercent() {
        return dupPercent;
    }

    public void setDupPercent(Double dupPercent) {
        this.dupPercent = dupPercent;
    }

    public Double getUniPercent() {
        return uniPercent;
    }

    public void setUniPercent(Double uniPercent) {
        this.uniPercent = uniPercent;
    }
}
