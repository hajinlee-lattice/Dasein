package com.latticeengines.domain.exposed.dataflow.flows;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.ReportColumn;

public class CreateReportParameters extends DataFlowParameters {

    @JsonProperty
    public String sourceTableName;

    @JsonProperty
    public List<ReportColumn> columns = new ArrayList<>();
}
