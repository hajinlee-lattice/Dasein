package com.latticeengines.serviceflows.workflow.stats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class ProfileArgument {

    @JsonProperty("IsProfile")
    private Boolean isProfile;

    @JsonProperty("DecodeStrategy")
    private BitDecodeStrategy decodeStrategy;

    @JsonProperty("BktAlgo")
    private String bktAlgo;

    @JsonProperty("NoBucket")
    private Boolean noBucket;

    @JsonProperty("NumBits")
    private Integer numBits;

    public boolean isProfile() {
        return Boolean.TRUE.equals(isProfile);
    }

    public BitDecodeStrategy getDecodeStrategy() {
        return decodeStrategy;
    }

    public String getBktAlgo() {
        return bktAlgo;
    }

    public boolean isNoBucket() {
        return Boolean.TRUE.equals(noBucket);
    }

    public Integer getNumBits() {
        return numBits;
    }

}
