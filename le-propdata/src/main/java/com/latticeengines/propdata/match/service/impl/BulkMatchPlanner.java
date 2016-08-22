package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.newrelic.api.agent.Trace;

@Component("bulkMatchPlanner")
public class BulkMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Trace
    public MatchContext plan(MatchInput input) {
        MatchContext context = new MatchContext();
        assignAndValidateColumnSelectionVersion(input);
        context.setInput(input);
        context.setColumnSelection(parseColumnSelection(input));
        context.setReturnUnmatched(input.getReturnUnmatched());
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output = initializeMatchOutput(input, null);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

}
