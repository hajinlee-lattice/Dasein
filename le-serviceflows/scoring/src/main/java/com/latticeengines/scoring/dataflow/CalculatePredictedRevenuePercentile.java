package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;

@Component("calculatePredictedRevenuePercentile")
public class CalculatePredictedRevenuePercentile
    extends TypesafeDataFlowBuilder<CalculatePredictedRevenuePercentileParameters> {

    private static final Logger log = LoggerFactory.getLogger(CalculatePredictedRevenuePercentile.class);

    private static final String SCORE_COUNT_FIELD_NAME = ScoreResultField.RawScore.displayName + "_count";

    @Override
    public Node construct(CalculatePredictedRevenuePercentileParameters parameters) {

        Node inputTable = addSource(parameters.getInputTableName());
        String percentileFieldName = parameters.getPercentileFieldName();
        Node addPercentileColumn = inputTable.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        FieldList retainedFields = new FieldList(addPercentileColumn.getFieldNames());

        String modelGuidFieldName = parameters.getModelGuidField();
        Map<String, String> originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        int minPct = parameters.getPercentileLowerBound();
        int maxPct = parameters.getPercentileUpperBound();

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            Node mergedScoreCount = mergeCount(addPercentileColumn, modelGuidFieldName);
            Node calculatePercentile = calculatePercentileByFieldMap(originalScoreFieldMap, modelGuidFieldName,
                                                                     percentileFieldName,
                                                                     minPct, maxPct, mergedScoreCount);
            return calculatePercentile.retain(retainedFields);
        } else {
            return addPercentileColumn;
        }
    }

    private Node calculatePercentileByFieldMap(Map<String, String> originalScoreFieldMap, //
                                               String modelGuidFieldName, String percentileFieldName,
                                               int minPct, int maxPct, Node mergedScoreCount) {

        Map<String, Node> nodes = splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            String originalScoreField = originalScoreFieldMap.getOrDefault(modelGuid, InterfaceName.RawScore.name());

            Node output = calculatePercentileByFieldName(modelGuidFieldName, originalScoreField, percentileFieldName,
                                                         minPct, maxPct, node);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;
    }

    private Map<String, Node> splitNodes(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        Map<String, Node> nodes = new HashMap<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidFieldName),
                                      new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String originalScoreFieldName,
                                                String percentileFieldName, int minPct, int maxPct, Node node) {

        if (ScoreResultField.RawScore.displayName.equals(originalScoreFieldName)) {
            return node;
        }

        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        Node calculatePercentile = node.groupByAndBuffer(
            new FieldList(modelGuidFieldName), new FieldList(originalScoreFieldName),
            new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])),
                                    minPct, maxPct, percentileFieldName, SCORE_COUNT_FIELD_NAME, originalScoreFieldName),
            true, returnedMetadata);
        return calculatePercentile;
    }


    private Node mergeCount(Node node, String modelGuidFieldName) {
        String scoreCountPipeName = "ModelScoreCount_" + UUID.randomUUID().toString().replace("-", "") + "_";
        String scoreFieldName = ScoreResultField.Percentile.displayName;
        Node score = node.retain(scoreFieldName, modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList(new FieldMetadata(modelGuidFieldName, String.class), //
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
