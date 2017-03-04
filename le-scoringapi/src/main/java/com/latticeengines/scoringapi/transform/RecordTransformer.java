package com.latticeengines.scoringapi.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.common.exposed.util.PrecisionUtils;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.TransformId;
import com.latticeengines.transform.exposed.TransformRetriever;

@Service
public class RecordTransformer {

    private static final Log log = LogFactory.getLog(RecordTransformer.class);

    @Autowired
    private TransformRetriever transformRetriever;

    @Autowired
    private JythonEngineRetriever jythonEngineRetriever;

    public Map<String, Object> transform(String modelPath, //
            List<TransformDefinition> definitions, //
            Map<String, Object> record) {

        JythonEngine engine = jythonEngineRetriever.getEngine(modelPath);
        Map<String, Object> result = new HashMap<>(record.size() + definitions.size());
        result.putAll(record);
        
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            if (entry.getValue() != null && entry.getValue() instanceof Double) {
                entry.setValue(PrecisionUtils.setPlatformStandardPrecision((Double) entry.getValue()));
            }
        }

        for (TransformDefinition entry : definitions) {
            TransformId id = new TransformId(modelPath, entry.name, null);
            try {
                RealTimeTransform transform = transformRetriever.getTransform(id);
                Object value = null;
                if (transform != null) {
                    value = transform.transform(entry.arguments, result);
                } else {
                    value = engine.invoke(entry.name, entry.arguments, result, entry.type.type());
                }

                if (value == null) {
                    value = null;
                } else if (entry.type.type() == Double.class) {
                    try {
                        if (value.toString().toLowerCase().equals("true")) {
                            value = entry.type.type().cast(Double.valueOf("1.0"));
                        } else if (value.toString().toLowerCase().equals("false")) {
                            value = entry.type.type().cast(Double.valueOf("0.0"));
                        } else if (!value.toString().equals("null") && !value.toString().equals("None")) {
                            value = new Double(
                                    PrecisionUtils.setPlatformStandardPrecision(Double.valueOf(value.toString())));
                        } else {
                            value = null;
                        }
                    } catch (Exception e) {
                        log.warn(String.format("Problem casting Transform value to Java Double"));
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

    public Map<String, Object> transformJython(String modelPath, List<TransformDefinition> definitions,
            Map<String, Object> record) {
        JythonEngine engine = jythonEngineRetriever.getEngine(modelPath);
        return transformJython(engine, definitions, record);
    }

    public Map<String, Object> transformJython(JythonEngine engine, List<TransformDefinition> definitions,
            Map<String, Object> record) {
        Map<String, Object> result = new HashMap<String, Object>(record.size() + definitions.size());
        result.putAll(record);

        for (TransformDefinition entry : definitions) {
            try {
                Object value = engine.invoke(entry.name, entry.arguments, result, entry.type.type());
                result.put(entry.output, value);
            } catch (Exception e) {
                log.warn(String.format("Problem invoking %s with args %s", entry.name, entry.arguments));
            }
        }

        return result;
    }

}
