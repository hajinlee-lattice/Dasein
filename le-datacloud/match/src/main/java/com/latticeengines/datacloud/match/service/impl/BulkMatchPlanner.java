package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.datacloud.manage.ColumnSelection;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;

@Component("bulkMatchPlanner")
public class BulkMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Override
    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @Override
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        MatchContext context = new MatchContext();
        assignAndValidateColumnSelectionVersion(input);
        context.setInput(input);
        ColumnSelection columnSelection = parseColumnSelection(input);
        context.setColumnSelection(columnSelection);
        context.setReturnUnmatched(input.getReturnUnmatched());
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output = initializeMatchOutput(input, columnSelection, null);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }
}
