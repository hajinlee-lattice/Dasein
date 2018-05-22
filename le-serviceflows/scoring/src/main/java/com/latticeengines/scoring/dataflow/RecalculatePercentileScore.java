package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculatePercentileScoreParameters;

@Component("recalculatePercentileScore")
public class RecalculatePercentileScore extends TypesafeDataFlowBuilder<RecalculatePercentileScoreParameters> {
    private static final Logger log = LoggerFactory.getLogger(GenerateTargetScorePercentileMap.class);

    public static final String SCORE_COUNT_FIELD_NAME = ScoreResultField.RawScore.displayName + "_count";

    @Override
    public Node construct(RecalculatePercentileScoreParameters parameters) {

        Node inputTable = addSource(parameters.getInputTableName());
        FieldList originalFields = new FieldList(inputTable.getFieldNames());

        String modelGuidFieldName = parameters.getModelGuidField();
        String rawScoreFieldName = parameters.getRawScoreFieldName();
        String scoreFieldName = parameters.getScoreFieldName();
        int minPct = parameters.getPercentileLowerBound();
        int maxPct = parameters.getPercentileUpperBound();

        Node mergedScoreCount = mergeCount(inputTable, modelGuidFieldName, scoreFieldName);

        Node calculatePercentile = mergedScoreCount.groupByAndBuffer(
            new FieldList(modelGuidFieldName), new FieldList(rawScoreFieldName),
            new CalculatePercentile(new Fields(mergedScoreCount.getFieldNames().toArray(new String[0])),
                                    minPct, maxPct, scoreFieldName, SCORE_COUNT_FIELD_NAME, rawScoreFieldName),
            true, new ArrayList<>(mergedScoreCount.getSchema()));

        return calculatePercentile.retain(originalFields);
    }

    private Node mergeCount(Node node, String modelGuidFieldName, String scoreFieldName) {
        String scoreCountPipeName = "ModelScoreCount_" + UUID.randomUUID().toString().replace("-", "") + "_";
        Node score = node.retain(scoreFieldName, modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                                                           new FieldMetadata(modelGuidFieldName, String.class), //
                                                           new FieldMetadata(scoreFieldName, String.class), //
                                                           new FieldMetadata(SCORE_COUNT_FIELD_NAME, Long.class) //
        );
        Node totalCount = score.groupByAndAggregate(new FieldList(modelGuidFieldName), //
                                                    new Count(new Fields(SCORE_COUNT_FIELD_NAME)), //
                                                    scoreCountFms, Fields.ALL) //
            .retain(modelGuidFieldName, SCORE_COUNT_FIELD_NAME);
        return node.innerJoin(modelGuidFieldName, totalCount, modelGuidFieldName);
    }
}
