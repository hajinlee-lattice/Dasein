package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("nodeSplitter")
public class NodeSplitter {

    public Map<String, Node> split(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        long timestamp = System.currentTimeMillis();
        Map<String, Node> nodes = new HashMap<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidFieldName),
                    new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid + timestamp);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    public Node[] splitRevenue(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        List<String> modelList = new ArrayList<>();
        List<String> revenueModelList = new ArrayList<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            boolean isNotRevenue = ScoreResultField.RawScore.displayName
                    .equals(originalScoreFieldMap.getOrDefault(modelGuid, ScoreResultField.RawScore.displayName));
            if (isNotRevenue) {
                modelList.add(modelGuid);
            } else {
                revenueModelList.add(modelGuid);
            }
        });
        Node model = createNode(input, modelGuidFieldName, modelList);
        Node revenueModel = createNode(input, modelGuidFieldName, revenueModelList);
        return new Node[] { model, revenueModel };
    }

    public Node[] splitEv(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        List<String> modelList = new ArrayList<>();
        List<String> predictedModelList = new ArrayList<>();
        List<String> evModelList = new ArrayList<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            splitModelNames(originalScoreFieldMap, modelList, predictedModelList, evModelList, modelGuid);
        });
        Node model = createNode(input, modelGuidFieldName, modelList);
        Node predictedModel = createNode(input, modelGuidFieldName, predictedModelList);
        Node evModel = createNode(input, modelGuidFieldName, evModelList);
        return new Node[] { model, predictedModel, evModel };
    }

    private void splitModelNames(Map<String, String> originalScoreFieldMap, List<String> modelList,
            List<String> predictedModelList, List<String> evModelList, String modelGuid) {
        boolean isNotRevenue = ScoreResultField.RawScore.displayName
                .equals(originalScoreFieldMap.getOrDefault(modelGuid, ScoreResultField.RawScore.displayName));
        if (isNotRevenue) {
            modelList.add(modelGuid);
        } else {
            boolean predicted = ScoreResultField.PredictedRevenue.displayName
                    .equals(originalScoreFieldMap.get(modelGuid));
            if (predicted) {
                predictedModelList.add(modelGuid);
            } else {
                evModelList.add(modelGuid);
            }
        }
    }

    private Node createNode(Node input, String modelGuidFieldName, List<String> modelList) {
        Node node = null;
        if (CollectionUtils.isNotEmpty(modelList)) {
            String names = String.join("|", modelList);
            node = input.filter(String.format("%s.matches(\"%s\")", modelGuidFieldName, names),
                    new FieldList(ScoreResultField.ModelId.displayName));
        }
        return node;
    }

}
