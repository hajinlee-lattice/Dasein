package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public abstract class ColumnToValueToDoubleLookup implements RealTimeTransform {
    
    private static final long serialVersionUID = 3553344738920319099L;

    protected static final Log log = LogFactory.getLog(Assignconversionrate.class);
    
    protected Map<String, Map<String, Double>> columnToValueToDoubleMap = new HashMap<>();

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        
        if (!columnToValueToDoubleMap.containsKey(column)) {
            throw new RuntimeException(String.format("%s is not in the lookup. Failing this call.", column));
        }
        
        Object value = record.get(column);
        String valueAsStr = null;
        if (value == null) {
            return 1.0;
        } else if (value instanceof Number) {
            valueAsStr = String.format("%.2f", (((Number) value)).doubleValue());
        } else if (value instanceof Boolean) {
            valueAsStr = (Boolean) value ? "1.00" : "0.00";
        } else {
            valueAsStr = value.toString();
        }
        if (!columnToValueToDoubleMap.get(column).containsKey(valueAsStr)) {
            return 1.0;
        }
        return columnToValueToDoubleMap.get(column).get(valueAsStr);
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }

    @SuppressWarnings({ "unchecked" })
    protected void importLookupMapFromJson(String filename) {
        try {
            String contents = FileUtils.readFileToString(new File(filename));
            columnToValueToDoubleMap = JsonUtils.deserialize(contents, Map.class, true);
        } catch (FileNotFoundException e) {
            log.warn(String.format("Cannot find json file with conversion rate mapping values: %s", filename));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with conversion rate mapping values: %s", filename), e);
        }
    }

}
