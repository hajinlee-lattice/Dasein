package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Node combinedResultTable = inputTable.leftOuterJoin(InterfaceName.Id.name(), scoreTable, InterfaceName.Id.name());

        List<String> fieldsToDiscard = new ArrayList<>();
        fieldsToDiscard.add(InterfaceName.InternalId.name());
        if(!parameters.isDebuggingEnabled()){
            fieldsToDiscard.add(ScoreResultField.RawScore.name());
        }
        return combinedResultTable.discard(new FieldList(fieldsToDiscard));
    }

}
