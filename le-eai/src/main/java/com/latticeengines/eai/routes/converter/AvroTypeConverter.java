package com.latticeengines.eai.routes.converter;

import org.apache.avro.Schema.Type;
import org.apache.camel.TypeConverter;
import org.apache.camel.spi.TypeConverterRegistry;

import com.latticeengines.common.exposed.util.AvroUtils;

public abstract class AvroTypeConverter {

	public abstract Type convertTypeToAvro(String type);
	
	public static Object convertIntoJavaValueForAvroType(TypeConverterRegistry typeRegistry, Type avroType, Object value) {
	    Class<?> targetType = null;
        switch (avroType) {
        case DOUBLE:
            targetType = Double.class;
            break;
        case FLOAT:
            targetType = Float.class;
            break;
        case INT:
            targetType = Integer.class;
            break;
        case LONG:
            targetType = Long.class;
            break;
        case STRING:
            targetType = String.class;
            break;
        case BOOLEAN:
            targetType = Boolean.class;
            break;
        case ENUM:
            targetType = String.class;
            break;
        default:
            break;
        }
        
        if (avroType == Type.ENUM) {
            value = AvroUtils.getAvroFriendlyString((String) value);
        }
        
        if (value.getClass().equals(targetType)) {
            return value;
        }
        
        TypeConverter converter = typeRegistry.lookup(targetType, value.getClass());
        
        
        return converter.convertTo(targetType, value);
	}
	
	public static Object getEmptyValue(Type avroType) {
        switch (avroType) {
        case DOUBLE:
            return 0d;
        case FLOAT:
            return 0f;
        case INT:
            return 0;
        case LONG:
            return 0L;
        case STRING:
            return "";
        case BOOLEAN:
            return Boolean.FALSE;
        case ENUM:
            return AvroUtils.getAvroFriendlyString(" ");
        default:
            break;
        }
        return new Object();
	}
	
}
