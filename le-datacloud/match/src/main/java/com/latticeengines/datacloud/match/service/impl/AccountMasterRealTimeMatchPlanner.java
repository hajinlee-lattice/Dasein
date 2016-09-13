package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;

@Component("accountMasterRealTimeMatchPlanner")
public class AccountMasterRealTimeMatchPlanner extends RealTimeMatchPlanner {

    @MatchStep
    @Override
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        validate(input);
        assignAndValidateColumnSelectionVersion(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        ColumnSelection columnSelection = parseColumnSelection(input);
        context.setColumnSelection(columnSelection);
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());
        context.setInput(input);
        MatchOutput output = initializeMatchOutput(input, columnSelection, metadatas);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }

    protected ColumnSelection parseColumnSelection(MatchInput input) {
        ColumnSelectionService columnSelectionService = getColumnSelectionService(input.getDataCloudVersion());

        if (input.getUnionSelection() != null) {
            return combineAccountMasterSelections(columnSelectionService, input.getUnionSelection());
        } else if (input.getPredefinedSelection() != null) {
            return columnSelectionService.parsePredefinedColumnSelection(input.getPredefinedSelection());
        } else {
            return input.getCustomSelection();
        }
    }

    @MatchStep(threshold = 100L)
    protected ColumnSelection combineAccountMasterSelections(ColumnSelectionService columnSelectionService,
            UnionSelection unionSelection) {
        List<ColumnSelection> selections = new ArrayList<>();
        for (Map.Entry<Predefined, String> entry : unionSelection.getPredefinedSelections().entrySet()) {
            Predefined predefined = entry.getKey();
            validateOrAssignPredefinedVersion(columnSelectionService, predefined, entry.getValue());
            selections.add(columnSelectionService.parsePredefinedColumnSelection(predefined));
        }
        if (unionSelection.getCustomSelection() != null && !unionSelection.getCustomSelection().isEmpty()) {
            selections.add(unionSelection.getCustomSelection());
        }
        return ColumnSelection.combine(selections);
    }

}
