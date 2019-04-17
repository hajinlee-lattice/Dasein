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

    public Node[] splitEv(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        List<String> modelList = new ArrayList<>();
        List<String> evModelList = new ArrayList<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            boolean isNotEV = ScoreResultField.RawScore.displayName.equals(originalScoreFieldMap.get(modelGuid));
            if (isNotEV) {
                modelList.add(modelGuid);
            } else {
                evModelList.add(modelGuid);
            }
        });
        Node model = createNode(input, modelGuidFieldName, modelList);
        Node evModel = createNode(input, modelGuidFieldName, evModelList);
        return new Node[] { model, evModel };
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
