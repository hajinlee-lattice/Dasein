package com.latticeengines.scoringapi.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

@Service
public class RecordTransformer {
    
    public Map<String, Object> transform(String modelPath, List<TransformDefinition> definitions, Map<String, Object> record) {
        Map<String, Object> result = new HashMap<String, Object>(record.size() + definitions.size());
        result.putAll(record);
        
        Integer recId = (Integer) record.get("Nutanix_EventTable_Clean");
        if (recId == 49692) {
            System.out.println("");
        }
        
        
        JythonEngine engine = retriever.getTransform(modelPath);
        
        for (TransformDefinition entry : definitions) {
            Object value = null;
            try {
                value = engine.invoke(entry.name, entry.arguments, record, entry.type.type());
            } catch (Exception e) {
                value = engine.invoke(entry.name, entry.arguments, record, entry.type.type());
            }
            result.put(entry.output, value);
        }

        return result;
    }
    
    @Autowired
    private TransformRetriever retriever;
}
