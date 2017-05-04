package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.leadprioritization.AddRatingColumnFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    private static final org.apache.commons.logging.Log log = LogFactory.getLog(CombineInputTableWithScore.class);

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Node scoreWithRating = scoreTable;
        boolean noRatingColumnInScoreTable = scoreWithRating.getSourceAttribute(ScoreResultField.Rating.displayName) == null ? true
                : false;
        boolean notPMMLModel = parameters.getModelType() == null ? true : parameters.getModelType().equals(
                ModelType.PYTHONMODEL.getModelType());

        if (noRatingColumnInScoreTable && notPMMLModel) {
            scoreWithRating = scoreTable.apply(new AddRatingColumnFunction(ScoreResultField.Percentile.displayName,
                    ScoreResultField.Rating.displayName, parameters.getBucketMetadata()), new FieldList(
                    ScoreResultField.Percentile.displayName), new FieldMetadata(ScoreResultField.Rating.displayName,
                    String.class));
        }

        Node combinedResultTable = null;
        String idColumn;
        if (inputTable.getSourceAttribute(InterfaceName.Id.name()) != null) {
            idColumn = InterfaceName.Id.name();
        } else {
            idColumn = InterfaceName.InternalId.name();
        }

        List<String> retainFields = new ArrayList<>(inputTable.getFieldNames());
        List<String> scoreWithRatingColumns = scoreWithRating.getFieldNames();
        scoreWithRatingColumns.remove(idColumn);
        retainFields.addAll(scoreWithRatingColumns);

        combinedResultTable = inputTable.leftJoin(idColumn, scoreWithRating, idColumn);
        combinedResultTable = combinedResultTable.groupByAndLimit(new FieldList(InterfaceName.InternalId.name()), 1);
        combinedResultTable = combinedResultTable.retain(new FieldList(retainFields));
        return combinedResultTable;
    }

}
