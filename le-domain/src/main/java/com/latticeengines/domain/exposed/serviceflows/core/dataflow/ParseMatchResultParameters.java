package com.latticeengines.domain.exposed.serviceflows.core.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class ParseMatchResultParameters extends DataFlowParameters {

    @JsonProperty
    @SourceTableName
    public String matchTableName;

    @JsonProperty
    @SourceTableName
    public String sourceTableName;

    @JsonProperty
    public List<String> sourceColumns;

    @JsonProperty
    public boolean excludeDataCloudAttrs;

    @JsonProperty
    public boolean keepLid;

    @JsonProperty
    public String matchGroupId;

    @JsonProperty
    public boolean joinInternalId;

}
