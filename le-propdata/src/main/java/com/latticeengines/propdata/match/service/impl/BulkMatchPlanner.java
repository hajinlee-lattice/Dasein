package com.latticeengines.propdata.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchPlanner;

@Component("bulkMatchPlanner")
public class BulkMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    private MatchContext matchContext;

    @MatchStep
    public MatchContext plan(MatchInput input) {
        if (matchContext == null) {
            matchContext = initializeMatchContext(input);
        } else {
            matchContext = setupForNewBlock(matchContext, input);
        }
        return matchContext;
    }

    @MatchStep
    private MatchContext initializeMatchContext(MatchInput input) {
        MatchContext context = new MatchContext();
        context.setInput(input);
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

    @MatchStep
    private MatchContext setupForNewBlock(MatchContext context, MatchInput input) {
        context.setInput(input);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        context = scanInputData(input, context);
        return context;
    }

}
