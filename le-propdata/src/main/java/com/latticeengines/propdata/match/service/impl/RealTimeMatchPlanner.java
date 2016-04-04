package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.RealTimeRequest;
import com.latticeengines.propdata.match.service.MatchPlanner;

@Component("realTimeMatchPlanner")
public class RealTimeMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Autowired
    private MetricService metricService;

    @Value("${propdata.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    @MatchStep
    public MatchContext plan(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        MatchContext context = new MatchContext();
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        context.setInput(input);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        context = generateInputMetric(context);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

    @MatchStep
    private MatchContext generateInputMetric(MatchContext context) {
        MatchInput input = context.getInput();
        Integer selectedCols = null;
        if (input.getPredefinedSelection() != null) {
            selectedCols = columnSelectionService.getTargetColumns(input.getPredefinedSelection()).size();
        }

        RealTimeRequest request = new RealTimeRequest(input, selectedCols);
        metricService.write(MetricDB.LDC_Match, request);

        return context;
    }

}
