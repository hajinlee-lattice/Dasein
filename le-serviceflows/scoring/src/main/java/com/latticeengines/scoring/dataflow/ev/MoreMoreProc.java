package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

@Component("moreMoreProc")
public class MoreMoreProc {

    @Inject
    private Helper helper;

    public Node mergeCount(ParsedContext context, Node node) {
        String scoreCountPipeName = "ModelScoreCount_" + UUID.randomUUID().toString().replace("-", "") + "_";
        String scoreFieldName = ScoreResultField.Percentile.displayName;
        Node score = node.retain(scoreFieldName, context.modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(context.modelGuidFieldName, String.class), //
                new FieldMetadata(scoreFieldName, String.class), //
                new FieldMetadata(context.scoreCountFieldName, Long.class) //
        );
        Node totalCount = score
                .groupByAndAggregate(new FieldList(context.modelGuidFieldName), //
                        new Count(new Fields(context.scoreCountFieldName)), //
                        scoreCountFms, Fields.ALL) //
                .retain(context.modelGuidFieldName, context.scoreCountFieldName);
        return node.innerJoin(context.modelGuidFieldName, totalCount, context.modelGuidFieldName);
    }

    public Node calculatePercentileByFieldMap(ParsedContext context, Node mergedScoreCount) {
        Map<String, Node> nodes = helper.splitNodes(mergedScoreCount, context.originalScoreFieldMap,
                context.modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            String originalScoreField = context.originalScoreFieldMap.getOrDefault(modelGuid,
                    InterfaceName.RawScore.name());

            Node output = calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
                    originalScoreField, context.percentileFieldName, context.minPct, context.maxPct, node);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;
    }

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String scoreCountFieldName,
            String originalScoreFieldName, String percentileFieldName, int minPct, int maxPct, Node node) {
        if (ScoreResultField.RawScore.displayName.equals(originalScoreFieldName)) {
            return node;
        }

        node = node.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        Node calculatePercentile = node
                .groupByAndBuffer(new FieldList(modelGuidFieldName), new FieldList(originalScoreFieldName),
                        new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])), minPct, maxPct,
                                percentileFieldName, scoreCountFieldName, originalScoreFieldName),
                        true, returnedMetadata);
        return calculatePercentile;
    }
}