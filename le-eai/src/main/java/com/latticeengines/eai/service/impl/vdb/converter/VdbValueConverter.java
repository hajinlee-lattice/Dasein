package com.latticeengines.eai.service.impl.vdb.converter;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.eai.service.ValueConverter;

public class VdbValueConverter implements ValueConverter {

    private static final Logger log = LoggerFactory.getLogger(VdbValueConverter.class);

    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertTo(Class<T> targetType, Object value) {
        if (!value.getClass().equals(String.class)) {
            throw new RuntimeException("Can not convert non String value of Vdb connector");
        }
        String valueStr = (String)value;
        if (targetType.equals(Double.class)) {
            return (T) new Double(valueStr);
        } else if (targetType.equals(Float.class)) {
            return (T) new Float(valueStr);
        } else if (targetType.equals(Integer.class)) {
            return (T) new Integer(valueStr);
        } else if (targetType.equals(Long.class)) {
            return (T) new Long(valueStr);
        } else if (targetType.equals(String.class)) {
            return (T) new String(valueStr);
        } else if (targetType.equals(Boolean.class)) {
            if (valueStr.equals("1") || valueStr.equalsIgnoreCase("true")) {
                return (T) Boolean.TRUE;
            } else {
                return (T) Boolean.FALSE;
            }
        } else {
            throw new IllegalArgumentException("Not supported target type: " + targetType.toString());
        }
    }

    @Override
    public String convertTimeStampString(Object value) {
        if (!value.getClass().equals(String.class)) {
            throw new RuntimeException("Can not convert non String value of Vdb connector");
        }
        String valueStr = (String)value;
        if (valueStr.matches("[0-9]+")) {
            return valueStr;
        } else {
            try {
                return Long.toString(TimeStampConvertUtils.convertToLong(valueStr));
            } catch (Exception e) {
                try {
                    DateTimeFormatter dtf = ISODateTimeFormat.dateTimeParser();
                    return Long.toString(dtf.parseDateTime(valueStr).getMillis());
                } catch (Exception e1) {
                    log.error("Vdb value converter cannot convert DataTime: " + valueStr);
                    throw e1;
                }
            }
        }
    }
}
