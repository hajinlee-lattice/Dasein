package com.latticeengines.scoringapi.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

@Service
public class RecordTransformer {

    private static final Log log = LogFactory.getLog(RecordTransformer.class);

    @Autowired
    private TransformRetriever retriever;

    public Map<String, Object> transform(String modelPath, List<TransformDefinition> definitions,
            Map<String, Object> record) {
        Map<String, Object> result = new HashMap<String, Object>(record.size() + definitions.size());
        result.putAll(record);

        JythonEngine engine = retriever.getTransform(modelPath);

        for (TransformDefinition entry : definitions) {
            Object value = null;
            boolean successfulInvocation = false;

            for (int numTries = 0; numTries < 2; numTries++) {
                try {
                    value = engine.invoke(entry.name, entry.arguments, record, entry.type.type());
                    successfulInvocation = true;
                } catch (Exception e) {
                    log.error(String.format("Problem invoking %s", entry.name), e);
                }
            }
            if (successfulInvocation) {
                record.put(entry.output, value);
                result.put(entry.output, value);
            }
        }

        log.info(JsonUtils.serialize(result));
        return result;
    }

}
