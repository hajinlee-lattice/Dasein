package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class ReplaceNullValue implements RealTimeTransform {

    private static final long serialVersionUID = -7933032058139360251L;

    protected Map<String, Object> lookupMap = new HashMap<>();

    public ReplaceNullValue() {
    }

    @SuppressWarnings("unchecked")
    public ReplaceNullValue(String modelPath) {
        try {
            String contents = FileUtils.readFileToString(new File(modelPath + "/imputations.txt"));
            lookupMap = JsonUtils.deserialize(contents, Map.class, true);
        } catch (IOException e) {
            throw new RuntimeException("Cannot build lookup.", e);
        }
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
    public TransformMetadata getMetadata() {
        return null;
    }

}