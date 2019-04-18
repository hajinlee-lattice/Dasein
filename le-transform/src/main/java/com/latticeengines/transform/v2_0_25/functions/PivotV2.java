package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class PivotV2 implements RealTimeTransform {

    private static final long serialVersionUID = 3246139112605001316L;

    protected Map<String, Object> lookupMap = new HashMap<>();

    public PivotV2() {
    }

    public PivotV2(String modelPath) {
        importLoookupMapFromJson(modelPath + "/pivotvalues.json");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column1");
        String targetColumn = (String) arguments.get("column2");
        List<?> values = (List) lookupMap.get(targetColumn);

        Object recordValue = record.get(column);
        if (targetColumn.endsWith("___ISNULL__")) {

            if (recordValue == null) {
                return 1.0;
            }
            return 0.0;
        }

        if (values != null && values.size() > 0) {
            List<?> pivotValues = (List) values.get(1);
            for (Object value : pivotValues) {
                if (value == null) {
                    continue;
                }
                if (value.equals(recordValue)) {
                    return 1.0;
                }
            }
        }
        return 0.0;

    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    public Map<String, Object> getLookupMap() {
        return lookupMap;
    }

    @SuppressWarnings("unchecked")
    private void importLoookupMapFromJson(String filename) {
        try {
            @SuppressWarnings("deprecation")
            String contents = FileUtils.readFileToString(new File(filename));
            lookupMap = JsonUtils.deserialize(contents, Map.class, true);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open json file with pivot values: %s", filename), e);
        }
    }

}
