package com.latticeengines.domain.exposed.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.annotation.AttributePropertyBag;

public class AttributeUtils {
    private static Logger log = Logger.getLogger(AttributeUtils.class);

    public static void copyPropertiesFromAttribute(Attribute source, Attribute dest) {
        copyPropertiesFromAttribute(source, dest, true);
    }

    public static void copyPropertiesFromAttribute(Attribute source, Attribute dest, boolean includeEmptySourceValues) {
        PropertyDescriptor[] descriptors = getPropertyDescriptors();

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getReadMethod() != null && descriptor.getWriteMethod() != null) {
                Object sourceValue = getValue(source, descriptor);
                Object destValue = getValue(dest, descriptor);
                if (!isPropertyBag(descriptor)) {
                    boolean sourceEmpty = sourceValue == null
                            || (sourceValue instanceof List && ((List<?>) sourceValue).size() == 0)
                            || (sourceValue instanceof Set && ((Set<?>) sourceValue).size() == 0);
                    if (includeEmptySourceValues || !sourceEmpty) {
                        setValue(dest, descriptor, sourceValue);
                        log.info(String.format("Setting property %s to be %s from source.  Value was previously %s",
                                descriptor.getName(), sourceValue, destValue));
                    } else {
                        log.debug(String.format("Ignoring property %s because it is null/empty on source",
                                descriptor.getName()));
                    }
                }
            }
        }
    }

    public static void setFieldMetadataFromAttribute(Attribute source, FieldMetadata fm) {
        setFieldMetadataFromAttribute(source, fm, true);
    }

    public static void setFieldMetadataFromAttribute(Attribute source, FieldMetadata fm,
            boolean includeEmptySourceValues) {
        PropertyDescriptor[] descriptors = getPropertyDescriptors();

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getReadMethod() != null && descriptor.getWriteMethod() != null) {
                Object sourceValue = getValue(source, descriptor);
                if (!isPropertyBag(descriptor)) {
                    boolean sourceEmpty = sourceValue == null
                            || (sourceValue instanceof List && ((List<?>) sourceValue).size() == 0)
                            || (sourceValue instanceof Set && ((Set<?>) sourceValue).size() == 0);
                    if (includeEmptySourceValues || !sourceEmpty) {
                        String key = StringUtils.substringAfter(descriptor.getWriteMethod().getName(), "set");
                        String metadataValue = String.valueOf(sourceValue);
                        String avroValue = fm.getPropertyValue(key);

                        if (avroValue != null && metadataValue != null && !avroValue.equals(metadataValue.toString())) {
                            log.warn(String
                                    .format("Property collision for field %s in Attribute %s. " //
                                            + "Value is %s in avro but %s in metadata table.  Using metadataValue from metadata table", //
                                            key, source.getName(), avroValue, metadataValue));
                        }
                        log.info(String.format("Setting property %s to be %s from source.", descriptor.getName(),
                                sourceValue));
                    } else {
                        log.debug(String.format("Ignoring property %s because it is null/empty on source",
                                descriptor.getName()));
                    }
                }
            }
        }
    }

    private static boolean isPropertyBag(PropertyDescriptor descriptor) {
        return descriptor.getWriteMethod().isAnnotationPresent(AttributePropertyBag.class);
    }

    public static void setPropertyFromString(Attribute attribute, String propertyName, String propertyValue) {
        try {
            Class<?> attrClass = Class.forName(Attribute.class.getName());
            String methodName = "set" + propertyName;
            Method m = null;
            try {
                m = attrClass.getMethod(methodName, String.class);
            } catch (Exception e) {
                // no method, skip
            }

            if (m != null) {
                try {
                    log.info(String.format("Setting property %s on attribute %s to be %s", propertyName,
                            attribute.getName(), propertyValue));
                    m.invoke(attribute, propertyValue);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.warn(
                            String.format("Failed to set property %s on attribute %s", propertyName,
                                    attribute.getName()), e);
                }
            }
        } catch (Exception e) {
            log.error(String.format("Failed to set properties on attribute %s", attribute.getName()), e);
        }

    }

    public static void setPropertiesFromStrings(Attribute attribute, Map<String, String> properties) {
        try {
            Class<?> attrClass = Class.forName(Attribute.class.getName());
            Map<String, Method> methodMap = new HashMap<>();

            for (String propertyName : properties.keySet()) {
                String methodName = "set" + propertyName;
                Method m = methodMap.get(methodName);

                if (m == null) {
                    try {
                        m = attrClass.getMethod(methodName, String.class);
                    } catch (Exception e) {
                        // no method, skip
                        continue;
                    }
                    methodMap.put(methodName, m);
                }

                if (m != null) {
                    try {
                        log.info(String.format("Setting property %s on attribute %s to be %s", propertyName,
                                attribute.getName(), properties.get(propertyName)));
                        m.invoke(attribute, properties.get(propertyName));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        log.warn(
                                String.format("Failed to set property %s on attribute %s", propertyName,
                                        attribute.getName()), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error(String.format("Failed to set properties on attribute %s", attribute.getName()), e);
        }
    }

    private static PropertyDescriptor[] getPropertyDescriptors() {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(Attribute.class);
            return beanInfo.getPropertyDescriptors();

        } catch (Exception e) {
            throw new RuntimeException("Failed to lookup property descriptors on Attribute", e);
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
