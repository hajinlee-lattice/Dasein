package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CalculateStatsParameter extends TransformationFlowParameters {

    @JsonProperty("enc_attrs")
    public List<DCEncodedAttr> encAttrs;

    @JsonProperty("ignore_attrs")
    public List<String> ignoreAttrs;

}
