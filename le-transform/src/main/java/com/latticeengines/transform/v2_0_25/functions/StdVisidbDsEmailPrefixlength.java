package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsEmailPrefixlength implements RealTimeTransform {

    private static final long serialVersionUID = 664716781231084878L;

    public StdVisidbDsEmailPrefixlength(String modelPath) {

    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return 0;

        String s = (String) o;

        if (s.indexOf("@") < 0)
            return 0;

        return s.indexOf("@");
    }

    @Override
    public Attribute getMetadata() {
        return null;
    }
}
