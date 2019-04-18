package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class ReplaceNullValueV2 implements RealTimeTransform {

    private static final long serialVersionUID = -830759481080944857L;

    protected Map<String, Object> lookupMap = new HashMap<>();

    public ReplaceNullValueV2() {
    }

    public ReplaceNullValueV2(String modelPath) {
        importLoookupMapFromJson(modelPath + "/imputations.json");
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);
        if (o == null) {
            Object value = lookupMap.get(column);
            if (value instanceof Integer) {
                return new Double((Integer) value);
            }
            return value;
        }
        return o;
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    @SuppressWarnings("unchecked")
    private void importLoookupMapFromJson(String filename) {
        try {
            @SuppressWarnings("deprecation")
            String contents = FileUtils.readFileToString(new File(filename));
            lookupMap = JsonUtils.deserialize(contents, Map.class, true);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open text file with imputation values: %s", filename), e);
        }
    }

}
