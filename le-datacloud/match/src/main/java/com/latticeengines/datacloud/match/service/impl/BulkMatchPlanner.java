package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
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
        if (OperationalMode.isEntityMatch(input.getOperationalMode())) {
            setEntityDecisionGraph(input);
        }
        setDataCloudVersion(input);

        MatchContext context = new MatchContext();
        context.setInput(input);
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }
        context.setMatchEngine(MatchContext.MatchEngine.BULK);

        MatchOutput output;
        ColumnSelection columnSelection;
        if (OperationalMode.ENTITY_MATCH.equals(input.getOperationalMode())) {
            // Handle Entity Match metadata and column selection setup.
            context.setCdlLookup(false);
            if (metadatas == null) {
                metadatas = parseEntityMetadata(input);
            }
            columnSelection = new ColumnSelection();
            List<Column> columns = metadatas.stream().map(cm -> new Column(cm.getAttrName())) //
                    .collect(Collectors.toList());
            columnSelection.setColumns(columns);
        } else {
            // Handle Legacy Match metadata and column selection setup.
            if (isAttrLookup(input)) {
                context.setCdlLookup(true);
                Pair<ColumnSelection, List<ColumnMetadata>> pair = setAttrLookupMetadata(context, input, metadatas);
                metadatas = pair.getRight();
                columnSelection = pair.getLeft();
            } else {
                context.setCdlLookup(false);
                columnSelection = parseColumnSelection(input);
                // TODO(lming, ysong): Should this be here? It is missing in real time case.
                metadatas = null;
            }
        }
        context.setColumnSelection(columnSelection);
        // TODO(lming, ysong): isCdlLookup false case not handled the same in Real Time
        // and Bulk. In bulk,
        // metadatas is always set to null but not in real time.
        output = initializeMatchOutput(input, columnSelection, metadatas);

        logger.info(String.format("Parsed %d columns in column selection", columnSelection.getColumns().size()));
        context.setOutput(output);
        if (OperationalMode.isEntityMatch(input.getOperationalMode())) {
            context = scanEntityInputData(input, context);
        } else {
            context = scanInputData(input, context);

        }
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }
}
