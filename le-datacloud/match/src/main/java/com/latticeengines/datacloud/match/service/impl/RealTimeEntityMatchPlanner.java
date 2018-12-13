package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;


@Component("realTimeEntityMatchPlanner")
public class RealTimeEntityMatchPlanner extends MatchPlannerBase implements MatchPlanner {

    @Value("${datacloud.match.realtime.max.input:1000}")
    private int maxRealTimeInput;


    public MatchContext plan(MatchInput input) {
        return plan(input, null, false);
    }

    @MatchStep
    public MatchContext plan(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        validate(input);

        setDataCloudVersion(input);
        setDecisionGraph(input);
        input.setNumRows(input.getData().size());
        MatchContext context = new MatchContext();
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        input.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());

        // This is sufficient condition for checking ID only since validation has confirmed that custom and union
        // column selection are null for CDL Match.
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            context.setSeekingIdOnly(true);
        }

        context.setCdlLookup(false);
        if (metadatas == null) {
            metadatas = parseColumnMetadata(input);
        }
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = metadatas.stream().map(cm -> new Column(cm.getAttrName())) //
                .collect(Collectors.toList());
        columnSelection.setColumns(columns);
        context.setColumnSelection(columnSelection);

        MatchOutput output = initializeMatchOutput(input, columnSelection, metadatas);
        context.setInput(input);
        context.setOutput(output);
        // TODO(dzheng): I believe we skip input data processing here and do it in the Entity Match Planner Actor?
        //context = scanInputData(input, context);
        return context;
    }

    List<ColumnMetadata> parseColumnMetadata(MatchInput input) {
        // For now, we only handle the Column Metadata case for a predefined column selection of ID.
        if (ColumnSelection.Predefined.ID.equals(input.getPredefinedSelection())) {
            List<ColumnMetadata> columnMetadataList = new ArrayList<>();
            ColumnMetadata atlasIdColumnMetadata = new ColumnMetadata();
            atlasIdColumnMetadata.setAttrName(InterfaceName.AtlasId.name());
            atlasIdColumnMetadata.setJavaClass(String.class.getSimpleName());
            columnMetadataList.add(atlasIdColumnMetadata);
            return columnMetadataList;
        } else {
            throw new UnsupportedOperationException("Column Metadata parsing for non-ID case is unsupported.");
        }
    }

    protected void validate(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }
}
