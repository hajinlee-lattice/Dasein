package com.latticeengines.eai.service.impl;

import org.apache.avro.Schema.Type;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.eai.service.ValueConverter;

public abstract class AvroTypeConverter {

    private static final Logger log = LoggerFactory.getLogger(AvroTypeConverter.class);

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

    public static Object convertIntoJavaValueForAvroType(ValueConverter valueConverter, Type avroType,
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

                if (attr.getSourceLogicalDataType().equalsIgnoreCase("Date")) {
                    dtf = ISODateTimeFormat.dateElementParser();
                } else if (attr.getSourceLogicalDataType().equalsIgnoreCase("Datetime")
                        || attr.getSourceLogicalDataType().equalsIgnoreCase("Timestamp")
                        || attr.getSourceLogicalDataType().equalsIgnoreCase("DateTimeOffset")) {
                    dtf = ISODateTimeFormat.dateTimeParser();
                } else {
                    break;
                }
                try {
                    DateTime dateTime = dtf.parseDateTime((String) value);
                    return dateTime.getMillis();
                } catch (Exception e) {
                    try {
                        return TimeStampConvertUtils.convertToLong((String) value);
                    } catch (Exception exp) {
                        log.error(String.format(
                                "Error parsing date using TimeStampConvertUtils for column %s with value %s.",
                                attr.getName(), value));
                    }
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

        return valueConverter.convertTo(targetType, value);
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
