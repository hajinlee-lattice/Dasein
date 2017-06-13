package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CopierParameters extends TransformationFlowParameters {

    @JsonProperty("retain_attrs")
    public List<String> retainAttrs;

    @JsonProperty("discard_attrs")
    public List<String> discardAttrs;

}
