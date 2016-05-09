package com.latticeengines.scoringapi.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.TransformId;
import com.latticeengines.transform.exposed.TransformRetriever;

@Service
public class RecordTransformer {

    private static final Log log = LogFactory.getLog(RecordTransformer.class);

    @Autowired
    private TransformRetriever transformRetriever;

    public Map<String, Object> transform(String modelPath, //
            List<TransformDefinition> definitions, //
            Map<String, Object> record) {

        Map<String, Object> result = new HashMap<>(record.size() + definitions.size());
        result.putAll(record);

        for (TransformDefinition entry : definitions) {
            TransformId id = new TransformId(modelPath, entry.name, null);
            try {
                RealTimeTransform transform = transformRetriever.getTransform(id);
                Object value = transform.transform(entry.arguments, result);

                if (value == null) {
                    value = null;
                } else if (entry.type.type() == Double.class) {
                    try {
                        if (value.toString().toLowerCase().equals("true") == true) {
                            value = entry.type.type().cast(Double.valueOf("1.0"));
                        } else if (value.toString().toLowerCase().equals("false") == true) {
                            value = entry.type.type().cast(Double.valueOf("0.0"));
                        } else if (value.toString().equals("null") == false && value.toString().equals("None") == false) {
                            value = entry.type.type().cast(Double.valueOf(value.toString()));
                        } else {
                            value = null;
                        }
                    } catch (Exception e) {
                        log.warn(String.format("Problem casting Transform value to Java Double"), e);
                    }
                }

                result.put(entry.output, value);
            } catch (Exception e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Problem invoking %s", entry.name), e);
                }
            }
        }

        return result;
    }

    public Map<String, Object> transformOld(String modelPath, List<TransformDefinition> definitions,
            Map<String, Object> record) {
        Map<String, Object> result = new HashMap<String, Object>(record.size() + definitions.size());
        result.putAll(record);
        JythonEngine engine = new JythonEngine(modelPath);

        for (TransformDefinition entry : definitions) {
            Object value = null;
            boolean successfulInvocation = false;
            Exception failedInvocationException = null;
            for (int numTries = 1; numTries < 3; numTries++) {
                try {
                    value = engine.invoke(entry.name, entry.arguments, result, entry.type.type());
                    successfulInvocation = true;
                    if (numTries > 1) {
                        log.warn(String.format("Transform invocation on %s succeeded on try #%d", entry.name, numTries));
                    }
                    break;
                } catch (Exception e) {
                    failedInvocationException = e;
                }
            }
            if (successfulInvocation) {
                result.put(entry.output, value);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Problem invoking %s", entry.name), failedInvocationException);
                }
            }
        }

        return result;
    }

}
