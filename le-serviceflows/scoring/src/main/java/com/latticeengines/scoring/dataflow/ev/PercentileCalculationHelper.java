package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.tuple.Fields;

@Component("percentileCalculationHelper")
public class PercentileCalculationHelper {

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node mergedScoreCount, boolean trySecondarySort) {
        Map<String, Node> nodes = nodeSplitter.split(mergedScoreCount, context.originalScoreFieldMap,
                context.modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            String originalScoreField = context.originalScoreFieldMap.getOrDefault(modelGuid,
                    InterfaceName.RawScore.name());
            String secondarySortFieldName = trySecondarySort ? context.outputExpRevFieldName : null;

            Node output = calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
                    originalScoreField, context.percentileFieldName, secondarySortFieldName, context.minPct,
                    context.maxPct, node);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;
    }

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String scoreCountFieldName,
            String originalScoreFieldName, String percentileFieldName, String secondarySortFieldName, int minPct,
            int maxPct, Node node) {
        if (ScoreResultField.RawScore.displayName.equals(originalScoreFieldName)) {
            return node;
        }

        node = node.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        FieldList sortFieldList = null;
        if (secondarySortFieldName == null || secondarySortFieldName.equals(originalScoreFieldName)) {
            sortFieldList = new FieldList(originalScoreFieldName);
        } else {
            sortFieldList = new FieldList(originalScoreFieldName, secondarySortFieldName);
        }

        Node calculatePercentile = node.groupByAndBuffer(new FieldList(modelGuidFieldName),
                sortFieldList, new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])), minPct,
                        maxPct, percentileFieldName, scoreCountFieldName, originalScoreFieldName),
                true, returnedMetadata);
        return calculatePercentile;
    }
}