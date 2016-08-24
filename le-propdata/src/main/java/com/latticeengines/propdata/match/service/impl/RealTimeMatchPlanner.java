package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.newrelic.api.agent.Trace;

@Component("realTimeMatchPlanner")
public class RealTimeMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Value("${propdata.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @MatchStep
    @Trace
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        validate(input);
        assignAndValidateColumnSelectionVersion(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        context.setColumnSelection(parseColumnSelection(input));
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());
        context.setInput(input);
        // TODO - this one calls parseColumnSelection twice... fix it
        MatchOutput output = initializeMatchOutput(input, metadatas);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }

    protected void validate(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }

}
