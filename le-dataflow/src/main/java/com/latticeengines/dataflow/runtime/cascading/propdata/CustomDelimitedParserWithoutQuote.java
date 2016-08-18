package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.lang.reflect.Type;

import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;

public class CustomDelimitedParserWithoutQuote extends DelimitedParser {
    private CsvToAvroFieldMapping fieldMap;

    public CustomDelimitedParserWithoutQuote(CsvToAvroFieldMapping fieldMap, String delimiter,
            boolean strict, boolean safe, FieldTypeResolver fieldTypeResolver) {
        super(delimiter, null, null, strict, safe, fieldTypeResolver);
        this.fieldMap = fieldMap;
    }

    @Override
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
