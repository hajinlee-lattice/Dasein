package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.leadprioritization.AddRatingColumnFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineInputTableWithScoreParameters;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    private static final Logger log = LoggerFactory.getLogger(CombineInputTableWithScore.class);

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Node scoreWithRating = scoreTable;
        boolean noRatingColumnInScoreTable = scoreWithRating
                .getSourceAttribute(ScoreResultField.Rating.displayName) == null ? true : false;
        boolean notPMMLModel = parameters.getModelType() == null ? true
                : parameters.getModelType().equals(ModelType.PYTHONMODEL.getModelType());

        if (noRatingColumnInScoreTable && notPMMLModel) {
            scoreWithRating = scoreTable.apply(
                    new AddRatingColumnFunction(ScoreResultField.Percentile.displayName,
                            ScoreResultField.Rating.displayName, parameters.getBucketMetadata()),
                    new FieldList(ScoreResultField.Percentile.displayName),
                    new FieldMetadata(ScoreResultField.Rating.displayName, String.class));
        }

        Node combinedResultTable = null;
        String idColumn;
        String groupByColumn = InterfaceName.InternalId.name();
        if (inputTable.getSourceAttribute(InterfaceName.Id.name()) != null) {
            idColumn = InterfaceName.Id.name();
        } else if (inputTable.getSourceAttribute(InterfaceName.InternalId.name()) != null) {
            idColumn = InterfaceName.InternalId.name();
        } else {
            idColumn = inputTable.getSourceSchema().getAttributes(LogicalDataType.InternalId).get(0).getName();
            groupByColumn = idColumn;
        }

        List<String> retainFields = new ArrayList<>(inputTable.getFieldNames());
        List<String> scoreWithRatingColumns = scoreWithRating.getFieldNames();
        scoreWithRatingColumns.forEach(e -> {
            if (!retainFields.contains(e))
                retainFields.add(e);
        });

        combinedResultTable = inputTable.leftJoin(idColumn, scoreWithRating, idColumn);
        combinedResultTable = combinedResultTable.groupByAndLimit(new FieldList(groupByColumn), 1);
        combinedResultTable = combinedResultTable.retain(new FieldList(retainFields));
        return combinedResultTable;
    }

}
