package com.latticeengines.propdata.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchPlanner;

@Component("bulkMatchPlanner")
public class BulkMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @MatchStep
    public MatchContext plan(MatchInput input) {
        MatchContext context = new MatchContext();
        assignAndValidateColumnSelectionVersion(input);
        context.setInput(input);
        context.setReturnUnmatched(input.getReturnUnmatched());
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

}
