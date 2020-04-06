package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

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
class ProfileArgument {

    @JsonProperty("IsProfile")
    Boolean isProfile = false;

    @JsonProperty("DecodeStrategy")
    BitDecodeStrategy decodeStrategy;

    @JsonProperty("BktAlgo")
    String bktAlgo;

    @JsonProperty("NoBucket")
    Boolean noBucket = false;

    @JsonProperty("NumBits")
    Integer numBits;

    public Boolean isProfile() {
        return Boolean.TRUE.equals(isProfile);
    }

    BitDecodeStrategy getDecodeStrategy() {
        return decodeStrategy;
    }

    String getBktAlgo() {
        return bktAlgo;
    }

    Boolean isNoBucket() {
        return Boolean.TRUE.equals(noBucket);
    }

    Integer getNumBits() {
        return numBits;
    }

}
