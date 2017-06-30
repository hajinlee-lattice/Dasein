package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FilterBucketedParameters extends TransformationFlowParameters {

    @JsonProperty("original_attrs")
    public List<String> originalAttrs;

    @JsonProperty("enc_attr_prefix")
    public String encAttrPrefix;

}
