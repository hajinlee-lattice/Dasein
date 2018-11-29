package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("realTimeMatchPlanner")
public class RealTimeMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Value("${datacloud.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @MatchStep
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        validate(input);
        if (isCdlLookup(input)) {
            if (StringUtils.isBlank(input.getDataCloudVersion())) {
                input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
            }
        }

        setDataCloudVersion(input);
        setDecisionGraph(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }
        MatchOutput output;

        if (isCdlLookup(input)) {
            context.setCdlLookup(true);
            if (metadatas == null) {
                metadatas = parseCDLMetadata(input);
            }
            ColumnSelection columnSelection = new ColumnSelection();
            List<Column> columns = metadatas.stream().map(cm -> new Column(cm.getAttrName())) //
                    .collect(Collectors.toList());
            columnSelection.setColumns(columns);
            context.setColumnSelection(columnSelection);
            context.setCustomAccountDataUnit(parseCustomAccount(input));
            context.setCustomDataUnits(parseCustomDynamo(input));
            output = initializeMatchOutput(input, columnSelection, metadatas);
        } else {
            context.setCdlLookup(false);
            ColumnSelection columnSelection = parseColumnSelection(input);
            context.setColumnSelection(columnSelection);
            output = initializeMatchOutput(input, columnSelection, metadatas);
        }

        context.setInput(input);
        context.setOutput(output);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context, skipExecutionPlanning);
        return context;
    }

    protected void validate(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }

}
