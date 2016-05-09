package com.latticeengines.transform.v2_0_25.functions;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Attribute;

public class Pivot extends Lookup {

    private static final long serialVersionUID = -3706913709622887686L;

    public Pivot() {
    }

    public Pivot(String modelPath) {
        super(modelPath + "/pivotvalues.txt", LookupType.StringToList);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column1");
        String targetColumn = (String) arguments.get("column2");
        List<?> values = (List) lookupMap.get(targetColumn);

        if (values != null && values.size() > 0) {
            List<?> pivotValues = (List) values.get(1);
            for (Object value : pivotValues) {
                if (value == null) {
                    continue;
                }
                if (value.equals(record.get(column))) {
                    return 1.0;
                }
            }
        }
        return 0.0;

    }

    @Override
    public Attribute getMetadata() {
        return null;
    }

}