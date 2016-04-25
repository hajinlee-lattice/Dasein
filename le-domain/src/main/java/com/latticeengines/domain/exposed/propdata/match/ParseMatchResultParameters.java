package com.latticeengines.domain.exposed.propdata.match;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class ParseMatchResultParameters extends DataFlowParameters {

    @JsonProperty
    public String sourceTableName;

    @JsonProperty
    public List<String> sourceColumns;

}
