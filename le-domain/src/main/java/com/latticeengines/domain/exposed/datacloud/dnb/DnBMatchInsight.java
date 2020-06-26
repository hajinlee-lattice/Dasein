package com.latticeengines.domain.exposed.datacloud.dnb;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DnBMatchInsight {

    @JsonProperty("NameMatchScore")
    private Double nameMatchScore;

    @JsonProperty("MatchGrade")
    private DnBMatchGrade matchGrade;

    @JsonProperty("ConfidenceCode")
    private Integer confidenceCode;

    @JsonProperty("MatchDataProfile")
    private DnBMatchDataProfile matchDataProfile;

    public Double getNameMatchScore() {
        return nameMatchScore;
    }

    public void setNameMatchScore(Double nameMatchScore) {
        this.nameMatchScore = nameMatchScore;
    }

    public DnBMatchGrade getMatchGrade() {
        return matchGrade;
    }

    public void setMatchGrade(DnBMatchGrade matchGrade) {
        this.matchGrade = matchGrade;
    }

    public Integer getConfidenceCode() {
        return confidenceCode;
    }

    public void setConfidenceCode(Integer confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    public DnBMatchDataProfile getMatchDataProfile() {
        return matchDataProfile;
    }

    public void setMatchDataProfile(DnBMatchDataProfile matchDataProfile) {
        this.matchDataProfile = matchDataProfile;
    }
}
