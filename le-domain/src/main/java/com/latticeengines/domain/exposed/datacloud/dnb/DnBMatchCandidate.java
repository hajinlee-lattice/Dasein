package com.latticeengines.domain.exposed.datacloud.dnb;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DnBMatchCandidate {

    public static class Attr {
        public static final String Classification = "Classification";
        public static final String MatchType = "MatchType";
        public static final String MatchedDuns = "MatchedDuns";
        public static final String ConfidenceCode = "ConfidenceCode";
        public static final String MatchGrade = "MatchGrade";
        public static final String MatchDataProfile = "MatchDataProfile";
        public static final String NameMatchScore = "NameMatchScore";
        public static final String OperatingStatusText = "OperatingStatusText";
        public static final String MatchPrimaryBusinessName = "MatchPrimaryBusinessName";
        public static final String MatchIso2CountryCode = "MatchIso2CountryCode";
    }

    @JsonProperty("MatchType")
    private String matchType;

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("NameLocation")
    private NameLocation nameLocation;

    @JsonProperty("OperatingStatus")
    private String OperatingStatus;

    @JsonProperty("MatchInsight")
    private DnBMatchInsight matchInsight;

    @JsonProperty("Classification")
    private Classification classification;

    @JsonProperty("IsMarketable")
    private Boolean isMarketable;

    @JsonProperty("IsMailUndeliverable")
    private Boolean isMailUndeliverable;

    @JsonProperty("IsUnreachable")
    private Boolean isUnreachable;

    @JsonProperty("FamilyTreeRoles")
    private List<String> familyTreeRoles;

    @JsonIgnore
    private Long matchDuration;

    public String getMatchType() {
        return matchType;
    }

    public void setMatchType(String matchType) {
        this.matchType = matchType;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public NameLocation getNameLocation() {
        return nameLocation;
    }

    public void setNameLocation(NameLocation nameLocation) {
        this.nameLocation = nameLocation;
    }

    public String getOperatingStatus() {
        return OperatingStatus;
    }

    public void setOperatingStatus(String operatingStatus) {
        OperatingStatus = operatingStatus;
    }

    public DnBMatchInsight getMatchInsight() {
        return matchInsight;
    }

    public void setMatchInsight(DnBMatchInsight matchInsight) {
        this.matchInsight = matchInsight;
    }

    public Classification getClassification() {
        return classification;
    }

    public void setClassification(Classification classification) {
        this.classification = classification;
    }

    public Long getMatchDuration() {
        return matchDuration;
    }

    public void setMatchDuration(Long matchDuration) {
        this.matchDuration = matchDuration;
    }

    public Boolean getMarketable() {
        return isMarketable;
    }

    public void setMarketable(Boolean marketable) {
        isMarketable = marketable;
    }

    public Boolean getMailUndeliverable() {
        return isMailUndeliverable;
    }

    public void setMailUndeliverable(Boolean mailUndeliverable) {
        isMailUndeliverable = mailUndeliverable;
    }

    public Boolean getUnreachable() {
        return isUnreachable;
    }

    public void setUnreachable(Boolean unreachable) {
        isUnreachable = unreachable;
    }

    public List<String> getFamilyTreeRoles() {
        return familyTreeRoles;
    }

    public void setFamilyTreeRoles(List<String> familyTreeRoles) {
        this.familyTreeRoles = familyTreeRoles;
    }

    public enum Classification {
        Accepted, Rejected
    }

}
