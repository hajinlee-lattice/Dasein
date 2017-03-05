package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketEncodeParameters extends TransformationFlowParameters {

    @JsonProperty("enc_attrs")
    public List<DCEncodedAttr> encAttrs;

    @JsonProperty("row_id_field")
    public String rowIdField;

}
