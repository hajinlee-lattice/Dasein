package com.latticeengines.propdata.collection.dataflow.buffer;

import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.PivotBuffer;

import cascading.operation.Buffer;
import cascading.tuple.Fields;

@SuppressWarnings("rawtypes")
public class FeaturePivotBuffer extends PivotBuffer implements Buffer {

    private static final long serialVersionUID = -1766121147776760904L;
    private static final String PIVOT_KEY = "Feature";
    private static final String PIVOT_VALUE = "Value";

    public FeaturePivotBuffer(List<Class> fieldFormats, Map<String, String> valueColumnMap, Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.pivotKeyField = PIVOT_KEY;
        this.pivotValueField = PIVOT_VALUE;
        this.valueColumnMap = valueColumnMap;
        this.fieldFormats = fieldFormats;
    }

    protected Object parseArgumentValue(Object value, Class type) {
        if (Integer.class.equals(type)) {
            return Integer.valueOf((String) value);
        } else if (Long.class.equals(type)) {
            return Long.valueOf((String) value);
        } else {
            return value;
        }
    }
}
