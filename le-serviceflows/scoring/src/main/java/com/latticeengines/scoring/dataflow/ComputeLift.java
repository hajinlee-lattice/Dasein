package com.latticeengines.scoring.dataflow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;

import cascading.operation.aggregator.Average;
import cascading.tuple.Fields;

@Component("computeLift")
public class ComputeLift extends TypesafeDataFlowBuilder<ComputeLiftParameters> {

    private String modelGuidField;
    private String liftField;
    private String ratingField;

    @Override
    public Node construct(ComputeLiftParameters parameters) {
        Node inputTable = addSource(parameters.getInputTableName());

        modelGuidField = parameters.getModelGuidField();
        liftField = parameters.getLiftField();
        ratingField = parameters.getRatingField();
        Map<String, String> scoreFieldMap = parameters.getScoreFieldMap();
        Map<String, Node> nodes = splitNodes(inputTable, scoreFieldMap);

        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();
            String scoreField = scoreFieldMap.get(modelGuid);
            Node lift = computeLift(node, modelGuid, scoreField);
            if (merged == null) {
                merged = lift;
            } else {
                merged = merged.merge(lift);
            }
        }

        return merged;
    }

    private Map<String, Node> splitNodes(Node input, Map<String, String> scoreFieldMap) {
        Map<String, Node> nodes = new HashMap<>();
        scoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidField),
                    new FieldList(modelGuidField));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    private Node computeLift(Node node, String modelGuid, String scoreField) {
        List<FieldMetadata> fms = Arrays.asList( //
                new FieldMetadata(modelGuidField, String.class), //
                new FieldMetadata(scoreField, Double.class), //
                new FieldMetadata("ModelAvg", Double.class) //
        );
        Node avg = node.retain(scoreField, modelGuidField) //
                .groupByAndAggregate(new FieldList(modelGuidField), new Average(new Fields("ModelAvg")), fms,
                        Fields.ALL);
        avg = avg.retain(modelGuidField, "ModelAvg").renamePipe(modelGuid + "_avg");
        fms = Arrays.asList( //
                new FieldMetadata(modelGuidField, String.class), //
                new FieldMetadata(ratingField, String.class), //
                new FieldMetadata(scoreField, Double.class), //
                new FieldMetadata("RatingAvg", Double.class) //
        );
        Node ratingAvg = node.retain(scoreField, modelGuidField, ratingField) //
                .groupByAndAggregate(new FieldList(modelGuidField, ratingField), new Average(new Fields("RatingAvg")), fms, Fields.ALL);
        ratingAvg = ratingAvg.retain(modelGuidField, ratingField, "RatingAvg").renamePipe(modelGuid + "_rating_avg");
        Node output = ratingAvg.innerJoin(modelGuidField, avg, modelGuidField);
        output = output.apply("RatingAvg / ModelAvg", new FieldList("RatingAvg", "ModelAvg"),
                new FieldMetadata(liftField, Double.class));
        return output.retain(modelGuidField, ratingField, liftField);
    }

}
