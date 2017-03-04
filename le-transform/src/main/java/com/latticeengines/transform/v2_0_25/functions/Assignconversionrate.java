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

public class Assignconversionrate implements RealTimeTransform {

    private static final long serialVersionUID = -7511040173923099149L;

    private static final Log log = LogFactory.getLog(Assignconversionrate.class);
    
    protected Map<String, Map<String, Double>> conversionRateMap = new HashMap<>();

    public Assignconversionrate() {
    }

    public Assignconversionrate(String modelPath) {
        importLoookupMapFromJson(modelPath + "/conversionratemapping.json");
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        
        if (!conversionRateMap.containsKey(column)) {
            throw new RuntimeException(String.format("%s is not in the conversion rate mapping lookup. Failing this call.", column));
        }
        
        Object value = record.get(column);
        String valueAsStr = null;
        if (value == null) {
            valueAsStr = "UNKNOWN";
        } else if (value instanceof Number) {
            valueAsStr = String.format("%.2f", (((Number) value)).doubleValue());
        } else {
            valueAsStr = value.toString();
        }
        if (!conversionRateMap.get(column).containsKey(valueAsStr)) {
            valueAsStr = "UNKNOWN";
        }
        return conversionRateMap.get(column).get(valueAsStr);
    }

    @Override
    public TransformMetadata getMetadata() {
        return null;
    }


    @SuppressWarnings({ "unchecked" })
    private void importLoookupMapFromJson(String filename) {
        try {
            String contents = FileUtils.readFileToString(new File(filename));
            conversionRateMap = JsonUtils.deserialize(contents, Map.class, true);
        } catch (FileNotFoundException e) {
            log.warn(String.format("Cannot find json file with conversion rate mapping values: %s", filename));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot open json file with conversion rate mapping values: %s", filename), e);
        }
    }

}