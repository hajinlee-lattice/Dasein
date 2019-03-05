package com.latticeengines.eai.service.impl;

import org.apache.camel.TypeConverter;
import org.apache.camel.spi.TypeConverterRegistry;

import com.latticeengines.eai.service.ValueConverter;

public class CamelValueConverter implements ValueConverter {

    private TypeConverterRegistry typeConverterRegistry;

    public CamelValueConverter(TypeConverterRegistry typeConverterRegistry) {
        this.typeConverterRegistry = typeConverterRegistry;
    }

    @Override
    public <T> T convertTo(Class<T> targetType, Object value) {
        TypeConverter converter = typeConverterRegistry.lookup(targetType, value.getClass());
        return converter.convertTo(targetType, value);
    }

    @Override
    public String convertTimeStampString(Object value) {
        return convertTo(String.class, value);
    }

    @Override
    public boolean autoFillNullValue() {
        return true;
    }
}
