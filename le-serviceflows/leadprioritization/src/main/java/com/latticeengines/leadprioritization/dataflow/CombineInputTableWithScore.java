package com.latticeengines.leadprioritization.dataflow;

import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        FieldMetadata id = Iterables.find(inputTable.getSchema(), new Predicate<FieldMetadata>() {
            @Override
            public boolean apply(FieldMetadata input) {
                if (input.getFieldName().equals(InterfaceName.Id.name())) {
                    return true;
                }
                return false;
            }

        }, null);
        Node combinedResultTable = null;
        if (id != null) {
            combinedResultTable = inputTable
                    .leftOuterJoin(InterfaceName.Id.name(), scoreTable, InterfaceName.Id.name());
        } else {
            combinedResultTable = inputTable.leftOuterJoin(InterfaceName.InternalId.name(), scoreTable,
                    InterfaceName.InternalId.name());
        }

        combinedResultTable = combinedResultTable.groupByAndLimit(new FieldList(InterfaceName.InternalId.name()), 1);
        return combinedResultTable;
    }

}
