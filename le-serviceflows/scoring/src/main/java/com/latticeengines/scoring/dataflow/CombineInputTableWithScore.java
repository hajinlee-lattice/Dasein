package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.leadprioritization.AddRatingColumnFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;

@Component("combineInputTableWithScore")
public class CombineInputTableWithScore extends TypesafeDataFlowBuilder<CombineInputTableWithScoreParameters> {

    private static final Logger log = LoggerFactory.getLogger(CombineInputTableWithScore.class);

    @Override
    public Node construct(CombineInputTableWithScoreParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Node scoreWithRating = scoreTable;
        boolean noRatingColumnInScoreTable = scoreWithRating
                .getSourceAttribute(ScoreResultField.Rating.displayName) == null;
        boolean notPMMLModel = parameters.getModelType() == null
                || parameters.getModelType().equals(ModelType.PYTHONMODEL.getModelType());

        if (StringUtils.isNotBlank(parameters.getModelIdField())) {
            log.info("Enter multi-model mode");
            Map<String, List<BucketMetadata>> bucketMetadataMap = parameters.getBucketMetadataMap();
            String modelIdField = parameters.getModelIdField();
            Map<String, String> scoreFieldMap = parameters.getScoreFieldMap();
            Map<String, Double> scoreAvgMap = parameters.getScoreAvgMap();
            Map<String, Integer> scoreMultiplierMap = parameters.getScoreMultiplierMap();
            List<String> applyToFields = new ArrayList<>();
            applyToFields.add(modelIdField);
            applyToFields.addAll(scoreFieldMap.values());
            scoreWithRating = scoreTable.apply(
                    new AddRatingColumnFunction(scoreFieldMap, modelIdField, ScoreResultField.Rating.displayName,
                            bucketMetadataMap, scoreMultiplierMap, scoreAvgMap),
                    new FieldList(applyToFields), new FieldMetadata(ScoreResultField.Rating.displayName, String.class));
        } else if (noRatingColumnInScoreTable && notPMMLModel) {
            log.info("Enter single-model mode");
            scoreWithRating = scoreTable.apply(
                    new AddRatingColumnFunction(parameters.getScoreFieldName(), ScoreResultField.Rating.displayName,
                            parameters.getBucketMetadata(), parameters.getScoreMultiplier(), parameters.getAvgScore()),
                    new FieldList(parameters.getScoreFieldName()),
                    new FieldMetadata(ScoreResultField.Rating.displayName, String.class));
        }

        Node combinedResultTable = null;
        String idColumn = InterfaceName.InternalId.name();
        String groupByColumn = InterfaceName.InternalId.name();
        if (inputTable.getSourceAttribute(InterfaceName.Id.name()) != null) {
            idColumn = InterfaceName.Id.name();
        } else if (inputTable.getSourceAttribute(InterfaceName.InternalId.name()) != null) {
            idColumn = InterfaceName.InternalId.name();
        }
        if (StringUtils.isNotEmpty(parameters.getIdColumn())) {
            idColumn = parameters.getIdColumn();
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
