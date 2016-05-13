package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class MakeFloat implements RealTimeTransform {

    private static final long serialVersionUID = -44152688011926836L;

    private static final Log log = LogFactory.getLog(MakeFloat.class);

    public MakeFloat() {
    }

    public MakeFloat(String modelPath) {
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);
        if (o == null) {
            return null;
        }
        try {
            return Double.valueOf(String.valueOf(o));
        } catch (Exception e) {
            log.error("Failed to cast " + o + " to Double.");
            return null;
        }
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

}
