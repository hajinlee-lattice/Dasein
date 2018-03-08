package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class MatchCdlMergeParameters extends DataFlowParameters {

    @JsonProperty("input_table_account_id")
    @SourceTableName
    public String tableWithAccountId;

    @JsonProperty("input_table_without_account_id")
    @SourceTableName
    public String tableWithoutAccountId;

    public MatchCdlMergeParameters() {
    }

    public MatchCdlMergeParameters(String tableWithAccountId, String tableWithoutAccountId) {
        this.tableWithAccountId = tableWithAccountId;
        this.tableWithoutAccountId = tableWithoutAccountId;
    }

}
