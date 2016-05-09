package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsEmailIsInvalid implements RealTimeTransform {

    private static final long serialVersionUID = -2544730034184720534L;

    public StdVisidbDsEmailIsInvalid(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return true;

        String s = (String) o;

        if (s.length() < 5)
            return true;

        if (!s.contains("@"))
            return true;

        return false;
    }

    @Override
    public Attribute getMetadata() {
        return null;
    }
}
