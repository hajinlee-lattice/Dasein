package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Node castedScoreTable = scoreTable.addFunction("String.valueOf(LeadID)", //
                new FieldList("LeadID"), //
                new FieldMetadata("LeadID_Str", Long.class));

        Node combinedResultTable = inputTable.leftOuterJoin(InterfaceName.Id.name(), castedScoreTable, "LeadID_Str");

        List<String> fieldsToDiscard = Arrays.<String>asList(new String[]{"LeadID", "LeadID_Str", "Play_Display_Name", //
                "Probability", "Score", "Lift", "Bucket_Display_Name"});

        if(!parameters.isDebuggingEnabled()){
            List<String> tmpList = fieldsToDiscard;
            fieldsToDiscard = new ArrayList<>();
            fieldsToDiscard.addAll(tmpList);
            fieldsToDiscard.add("RawScore");
        }
        return combinedResultTable.discard(new FieldList(fieldsToDiscard));
    }

}
