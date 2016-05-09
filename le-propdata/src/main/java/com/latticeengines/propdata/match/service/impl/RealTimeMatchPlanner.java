package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchPlanner;

@Component("realTimeMatchPlanner")
public class RealTimeMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Value("${propdata.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    @MatchStep
    public MatchContext plan(MatchInput input) {
        validate(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());
        context.setInput(input);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

    @MatchStep
    private void validate(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }

}
