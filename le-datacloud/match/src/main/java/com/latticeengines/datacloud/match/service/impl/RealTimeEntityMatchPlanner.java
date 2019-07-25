package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("realTimeEntityMatchPlanner")
public class RealTimeEntityMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Value("${datacloud.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    @Override
    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @Override
    @MatchStep
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        validate(input);

        setDataCloudVersion(input);
        setEntityDecisionGraph(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());

        // This is sufficient condition for checking ID only since validation has
        // confirmed that custom and union
        // column selection are null for CDL Match.
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }

        ColumnSelection columnSelection;
        if (isAttrLookup(input)) {
            context.setCdlLookup(true);
            Pair<ColumnSelection, List<ColumnMetadata>> pair = setAttrLookupMetadata(context, input, metadatas);
            metadatas = pair.getRight();
            columnSelection = pair.getLeft();
        } else {
            context.setCdlLookup(false);
            if (metadatas == null) {
                metadatas = parseEntityMetadata(input);
            }
            columnSelection = new ColumnSelection();
            List<Column> columns = metadatas.stream().map(cm -> new Column(cm.getAttrName())) //
                    .collect(Collectors.toList());
            columnSelection.setColumns(columns);
        }
        context.setColumnSelection(columnSelection);

        MatchOutput output = initializeMatchOutput(input, columnSelection, metadatas);
        context = scanEntityInputData(input, context);
        context.setInput(input);
        context.setOutput(output);

        return context;
    }

    protected void validate(MatchInput input) {
        DecisionGraph decisionGraph = findDecisionGraph(input);
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput, decisionGraph);
    }

}
