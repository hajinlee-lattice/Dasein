package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.lang.reflect.Type;

import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;

public class CustomDelimitedParser extends DelimitedParser {
    private static final long serialVersionUID = -1832800305799711109L;
	private CsvToAvroFieldMapping fieldMap;

    public CustomDelimitedParser(CsvToAvroFieldMapping fieldMap, String delimiter, String quote, boolean strict,
            boolean safe, FieldTypeResolver fieldTypeResolver) {
        super(delimiter, quote, null, strict, safe, fieldTypeResolver);
        this.fieldMap = fieldMap;
    }

    protected Type[] inferTypes(Object[] result) {
        if (result == null || result.length == 0) {
            return null;
        }
        Type[] types = new Type[result.length];
        for (int i = 0; i < result.length; i++) {
            types[i] = fieldMap.getFieldType((String) result[i]);

        }
        return types;
    }
}
