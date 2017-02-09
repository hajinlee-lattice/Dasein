package com.latticeengines.leadprioritization.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());
        Node scoreWithRating = scoreTable;
        if (scoreWithRating.getSourceAttribute(ScoreResultField.Rating.displayName) == null) {
            scoreWithRating = scoreTable.apply(
                    new AddRatingColumnFunction(ScoreResultField.Percentile.displayName,
                            ScoreResultField.Rating.displayName, parameters.getBucketMetadata()),
                    new FieldList(ScoreResultField.Percentile.displayName),
                    new FieldMetadata(ScoreResultField.Rating.displayName, String.class));
        }

        Node combinedResultTable = null;
        if (inputTable.getSourceAttribute(InterfaceName.Id.name()) != null) {
            combinedResultTable = inputTable.leftOuterJoin(InterfaceName.Id.name(), scoreWithRating,
                    InterfaceName.Id.name());
        } else {
            combinedResultTable = inputTable.leftOuterJoin(InterfaceName.InternalId.name(), scoreWithRating,
                    InterfaceName.InternalId.name());
        }

        combinedResultTable = combinedResultTable.groupByAndLimit(new FieldList(InterfaceName.InternalId.name()), 1);
        return combinedResultTable;
    }

}
