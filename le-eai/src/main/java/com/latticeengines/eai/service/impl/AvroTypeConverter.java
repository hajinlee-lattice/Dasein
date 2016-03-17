package com.latticeengines.eai.service.impl;

import org.apache.avro.Schema.Type;
import org.apache.camel.TypeConverter;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;

public abstract class AvroTypeConverter {

    private static final Log log = LogFactory.getLog(AvroTypeConverter.class);

    public Type convertTypeToAvro(String type) {

        switch (type) {
        case "Int":
            type = "java.lang.Integer";
            break;
        case "Date":
            type = "java.util.Date";
            break;
        case "Timestamp":
            type = "java.sql.Timestamp";
            break;
        default:
            type = "java.lang." + type;

        }
        Class<?> javaType = null;
        try {
            javaType = Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return AvroUtils.getAvroType(javaType);
    }

    public static Object convertIntoJavaValueForAvroType(TypeConverterRegistry typeRegistry, Type avroType,
            Attribute attr, Object value) {
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
            DateTimeFormatter dtf = null;

            if (attr.getSourceLogicalDataType().equals("date") || attr.getSourceLogicalDataType().equals("Date")) {
                dtf = ISODateTimeFormat.dateElementParser();
            } else if (attr.getSourceLogicalDataType().equals("datetime") || attr.getSourceLogicalDataType().equals("Timestamp")) {
                dtf = ISODateTimeFormat.dateTimeParser();
            }
            try {
                DateTime dateTime = dtf.parseDateTime((String) value);
                return dateTime.getMillis();
            } catch (Exception e) {
                log.warn(String.format("Error parsing date for column %s with value %s.", attr.getName(), value), e);
            }
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
