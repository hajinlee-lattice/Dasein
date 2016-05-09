package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Attribute;

public class ReplaceNullValue extends Lookup {

    private static final long serialVersionUID = -7933032058139360251L;

    public ReplaceNullValue() {
    }

    public ReplaceNullValue(String modelPath) {
        super(modelPath + "/imputations.txt", LookupType.StringToValue);
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);
        if (o == null) {
            return lookupMap.get(column);
        }
        return o;
    }

    @Override
    public Attribute getMetadata() {
        return null;
    }

}