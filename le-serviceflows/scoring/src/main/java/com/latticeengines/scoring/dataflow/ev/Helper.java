package com.latticeengines.scoring.dataflow.ev;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.operation.aggregator.Average;
import cascading.tuple.Fields;

@Component("helper")
public class Helper {

    public Map<String, Node> splitNodes(Node input, Map<String, String> originalScoreFieldMap,
            String modelGuidFieldName) {
        Map<String, Node> nodes = new HashMap<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidFieldName),
                    new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    public Node doSomethingHere(ParsedContext context, Node calculatePercentile) {
        // 4. for each percentile bucket calculate average expected
        // revenue
        //
        // 5. Use "ev" field from evScoreDerivation.json to lookup for
        // temporary EV percentile score for average expected revenue
        // value

        FieldList outputFields = new FieldList("PercentileField", "expectedRevenue");
        Node result = calculatePercentile.retain(outputFields);
        FieldList groupByFieldList = new FieldList(context.modelGuidFieldName);
        String scoreFieldName = ScoreResultField.Percentile.displayName;
        // Node score = node.retain(scoreFieldName,
        // modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(context.modelGuidFieldName, String.class), //
                new FieldMetadata(scoreFieldName, String.class), //
                new FieldMetadata(context.scoreCountFieldName, Long.class));
        result.groupByAndAggregate(groupByFieldList, new Average(new Fields(context.scoreCountFieldName)), //
                scoreCountFms, Fields.ALL);
        // calculate average expected revenue for modelId-percentile pair (for
        // EV models) and then join this column back to actual data
        // then use the fitter fuction to go throw each record to lookup
        // temporary EV percentile score for average expected revenue value
        // and then use this percentile score to use fit function and comeup
        // with fitted EV value
        return calculatePercentile;
    }
}
