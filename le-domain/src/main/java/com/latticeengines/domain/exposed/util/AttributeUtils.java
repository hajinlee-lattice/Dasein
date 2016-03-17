package com.latticeengines.domain.exposed.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;

import com.latticeengines.domain.exposed.metadata.Attribute;

public class AttributeUtils {
    private static Logger log = Logger.getLogger(AttributeUtils.class);

    public static void mergeAttributes(Attribute source, Attribute dest) {
        PropertyDescriptor[] properties;
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(Attribute.class);
            properties = beanInfo.getPropertyDescriptors();

        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to copy metadata from attribute %s to attribute %s",
                    source.getName(), dest.getName()), e);
        }

        for (PropertyDescriptor property : properties) {
            if (property.getReadMethod() != null && property.getWriteMethod() != null) {

                Object destValue = getValue(dest, property);
                if (destValue == null) {
                    Object sourceValue = getValue(source, property);
                    if (sourceValue != null) {
                        setValue(dest, property, sourceValue);
                        log.info(String.format("Setting property %s to be %s from source", property.getName(),
                                sourceValue));
                    } else {
                        log.debug(String.format(
                                "Ignoring property %s because both source and dest properties are null",
                                property.getName()));
                    }
                } else {
                    log.debug(String.format(String.format(
                            "Ignoring property %s because the value is already defined on dest to be %s",
                            property.getName(), destValue)));
                }
            }
        }
    }

    private static Object getValue(Attribute attribute, PropertyDescriptor property) {
        Object destValue = null;
        try {
            destValue = property.getReadMethod().invoke(attribute);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            // warn
        }
        return destValue;
    }

    private static void setValue(Attribute attribute, PropertyDescriptor property, Object value) {
        try {
            property.getWriteMethod().invoke(attribute, value);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            // warn
        }
    }
}
