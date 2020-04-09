package com.latticeengines.domain.exposed.datacloud.dnb;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
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

    @JsonProperty("DUNS")
    private String duns;

    @JsonProperty("NameLocation")
    private NameLocation nameLocation;

    @JsonProperty("OperatingStatus")
    private String OperatingStatus;

    @JsonProperty("MatchInsight")
    private DnBMatchInsight matchInsight;

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
}
