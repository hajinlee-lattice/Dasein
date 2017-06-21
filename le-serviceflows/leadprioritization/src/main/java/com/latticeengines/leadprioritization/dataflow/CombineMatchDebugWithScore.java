package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineMatchDebugWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("combineMatchDebugWithScore")
public class CombineMatchDebugWithScore extends TypesafeDataFlowBuilder<CombineMatchDebugWithScoreParameters> {

    @Override
    public Node construct(CombineMatchDebugWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        String idColumn = null;
        if (inputTable.getSourceAttribute(InterfaceName.Id.name()) != null) {
            idColumn = InterfaceName.Id.name();
        } else {
            idColumn = InterfaceName.InternalId.name();
        }
        List<String> columnsToRetain = new ArrayList<>();
        if (!parameters.getColumnsToRetain().contains(idColumn)) {
            columnsToRetain.add(idColumn);
        }
        columnsToRetain.addAll(parameters.getColumnsToRetain());

        inputTable = inputTable.retain(new FieldList(columnsToRetain));
        return scoreTable.leftJoin(idColumn, inputTable, idColumn);

    }
}
