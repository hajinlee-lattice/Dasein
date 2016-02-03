package com.latticeengines.skald;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

@Service
public class RecordTransformer {
    public Map<String, Object> transform(List<TransformDefinition> definitions, Map<String, Object> record) {
        Map<String, Object> result = new HashMap<String, Object>(record.size() + definitions.size());
        result.putAll(record);

        for (TransformDefinition entry : definitions) {
            JythonTransform transform = retriever.getTransform(entry.name);
            Object value = transform.invoke(entry.arguments, result, entry.type.type());
            result.put(entry.output, value);
        }

        return result;
    }

    @Autowired
    private TransformRetriever retriever;
}
