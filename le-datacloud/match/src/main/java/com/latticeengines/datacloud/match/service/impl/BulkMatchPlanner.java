package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("bulkMatchPlanner")
public class BulkMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    private static final Logger logger = LoggerFactory.getLogger(BulkMatchPlanner.class);

    @Override
    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @Override
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        MatchContext context = new MatchContext();
        context.setInput(input);
        ColumnSelection columnSelection = parseColumnSelection(input);
        logger.info(String.format("Parsed %d columns in column selection", columnSelection.getColumnIds().size()));
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }
        context.setColumnSelection(columnSelection);
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output = initializeMatchOutput(input, columnSelection, null);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }
}
