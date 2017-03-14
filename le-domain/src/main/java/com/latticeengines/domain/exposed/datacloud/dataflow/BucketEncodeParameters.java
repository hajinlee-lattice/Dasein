package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketEncodeParameters extends TransformationFlowParameters {

    @JsonProperty("enc_attrs")
    public List<DCEncodedAttr> encAttrs;

    @JsonProperty("exclude_attrs")
    public List<String> excludeAttrs;


    // legacy configuration, just to rename LatticeId to LatticeAccountId
    @JsonProperty("row_id_field")
    public String rowIdField;
    @JsonProperty("rename_row_id_field")
    public String renameRowIdField;

}
