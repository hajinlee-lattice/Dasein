package com.latticeengines.cdl.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlMergeParameters;

@Component("matchCdlMergeFlow")
public class MatchCdlMergeFlow extends TypesafeDataFlowBuilder<MatchCdlMergeParameters> {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlMergeFlow.class);

    @Override
    public Node construct(MatchCdlMergeParameters parameters) {

        Node inputTableWithAccountId = parameters.tableWithAccountId != null ? addSource(parameters.tableWithAccountId)
                : null;
        Node inputTableWithoutAccountId = parameters.tableWithoutAccountId != null
                ? addSource(parameters.tableWithoutAccountId) : null;
        if (inputTableWithAccountId == null)
            return inputTableWithoutAccountId;
        if (inputTableWithoutAccountId == null)
            return inputTableWithAccountId;

        log.info("Merging tables: table with account Id=" + parameters.tableWithAccountId + " table without account Id="
                + parameters.tableWithoutAccountId);
        return inputTableWithAccountId.merge(inputTableWithoutAccountId);
    }

}
