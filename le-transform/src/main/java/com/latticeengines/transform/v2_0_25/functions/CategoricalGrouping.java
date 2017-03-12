package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

public class CategoricalGrouping extends ColumnToValueToDoubleLookup {

    private static final long serialVersionUID = 3582964446134731109L;

    public CategoricalGrouping() {
    }

    public CategoricalGrouping(String modelPath) {
        importLookupMapFromJson(modelPath + "/dscategvargroupinginfo.json");
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        
        if (!columnToValueToDoubleMap.containsKey(column)) {
            throw new RuntimeException(String.format("%s is not in the lookup. Failing this call.", column));
        }
        
        Object value = record.get(column);

        if (value == null) {
            record.put(column, "LE-missing");
        }
        return super.transform(arguments, record);
    }

}
