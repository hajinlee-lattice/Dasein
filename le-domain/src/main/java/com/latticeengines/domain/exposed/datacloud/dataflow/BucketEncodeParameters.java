package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketEncodeParameters extends TransformationFlowParameters {

    @JsonProperty("enc_attrs")
    public List<DCEncodedAttr> encAttrs;

    @JsonProperty("retain_attrs")
    public List<String> retainAttrs;

    @JsonProperty("rename_fields")
    public Map<String, String> renameFields;

    @JsonProperty("profile_src_idx")
    public int profileSrcIdx;

}
