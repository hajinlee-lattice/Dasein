package com.latticeengines.scoring.dataflow.ev;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("nodeSplitter")
public class NodeSplitter {

    public Map<String, Node> split(Node input, Map<String, String> originalScoreFieldMap,
            String modelGuidFieldName) {
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

}
