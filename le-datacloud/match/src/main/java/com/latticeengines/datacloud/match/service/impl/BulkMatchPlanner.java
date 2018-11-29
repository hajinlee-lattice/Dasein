package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
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
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }
        context.setMatchEngine(MatchContext.MatchEngine.BULK);
        MatchOutput output;
        ColumnSelection columnSelection;
        if (isCdlLookup(input)) {
            context.setCdlLookup(true);
            if (metadatas == null) {
                metadatas = parseCDLMetadata(input);
            }
            columnSelection = new ColumnSelection();
            List<Column> columns = metadatas.stream().map(cm -> new Column(cm.getAttrName())) //
                    .collect(Collectors.toList());
            columnSelection.setColumns(columns);
            context.setColumnSelection(columnSelection);
            context.setCustomAccountDataUnit(parseCustomAccount(input));
            context.setCustomDataUnits(parseCustomDynamo(input));
            output = initializeMatchOutput(input, columnSelection, metadatas);
        } else {
            context.setCdlLookup(false);
            columnSelection = parseColumnSelection(input);
            output = initializeMatchOutput(input, columnSelection, null);
        }
        context.setColumnSelection(columnSelection);
        logger.info(String.format("Parsed %d columns in column selection", columnSelection.getColumns().size()));
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }
}
