package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccountMasterIntermediateSeedParameters extends TransformationFlowParameters {
    @JsonProperty("StandardCountries")
    private Map<String, String> standardCountries;

    public Map<String, String> getStandardCountries() {
        return standardCountries;
    }

    public void setStandardCountries(Map<String, String> standardCountries) {
        this.standardCountries = standardCountries;
    }

}
