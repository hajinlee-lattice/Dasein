package com.latticeengines.domain.exposed.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.annotation.AttributePropertyBag;

public class AttributeUtils {
    private static Logger log = LoggerFactory.getLogger(AttributeUtils.class);

    public static void copyPropertiesFromAttribute(Attribute source, Attribute dest) {
        copyPropertiesFromAttribute(source, dest, true);
    }

    public static void copyPropertiesFromAttribute(Attribute source, Attribute dest,
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
                        setValue(dest, descriptor, sourceValue);
                        // log.info(String.format("Setting property %s on
                        // attribute %s to be %s from source %s. Value was
                        // previously %s",
                        // descriptor.getName(), dest.getName(), sourceValue,
                        // source.getName(), destValue));
                    } else {
                        log.debug(String.format(
                                "Ignoring property %s on attribute %s because it is null/empty on source %s",
                                descriptor.getName(), dest.getName(), source.getName()));
                    }
                }
            }
        }
    }

    public static HashSet<String> diffBetweenAttributes(Attribute base, Attribute target) {
        HashSet<String> diffFields = new HashSet<>();
        PropertyDescriptor[] descriptors = getPropertyDescriptors();
        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getReadMethod() != null && descriptor.getWriteMethod() != null
                    && !isPropertyBag(descriptor)) {
                Object targetValue = getValue(target, descriptor);
                Object baseValue = getValue(base, descriptor);
                boolean targetValueEmpty = targetValue == null
                        || (targetValue instanceof List && ((List<?>) targetValue).size() == 0)
                        || (targetValue instanceof Set && ((Set<?>) targetValue).size() == 0);
                boolean baseValueEmpty = baseValue == null
                        || (baseValue instanceof List && ((List<?>) baseValue).size() == 0)
                        || (baseValue instanceof Set && ((Set<?>) baseValue).size() == 0);
                if (!targetValueEmpty) {
                    if (baseValueEmpty) {
                        diffFields.add(descriptor.getDisplayName().toLowerCase());
                    } else {
                        boolean equal;
                        if (targetValue instanceof String) {
                            equal = StringUtils.equals((String) targetValue, (String) baseValue);
                        } else {
                            equal = EqualsBuilder.reflectionEquals(targetValue, baseValue);
                        }
                        if (!equal) {
                            diffFields.add(descriptor.getDisplayName().toLowerCase());
                        }
                    }
                }
            }
        }
        return diffFields;
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
                        String key = StringUtils
                                .substringAfter(descriptor.getWriteMethod().getName(), "set");
                        String metadataValue = String.valueOf(sourceValue);
                        String avroValue = fm.getPropertyValue(key);

                        if (avroValue != null && metadataValue != null
                                && !avroValue.equals(metadataValue.toString())) {
                            log.warn(String.format(
                                    "Property collision for field %s in Attribute %s. " //
                                            + "Value is %s in avro but %s in metadata table.  Using metadataValue from metadata table", //
                                    key, source.getName(), avroValue, metadataValue));
                        }
                        fm.setPropertyValue(key, metadataValue);
                        // log.info(String.format("Setting property %s to be %s
                        // from source.", descriptor.getName(),
                        // sourceValue));
                    } else {
                        log.debug(String.format(
                                "Ignoring property %s because it is null/empty on source",
                                descriptor.getName()));
                    }
                }
            }
        }
    }

    private static boolean isPropertyBag(PropertyDescriptor descriptor) {
        return descriptor.getWriteMethod().isAnnotationPresent(AttributePropertyBag.class);
    }

    public static void setPropertyFromString(Attribute attribute, String propertyName,
            String propertyValue) {
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
                    // log.info(String.format("Setting property %s on attribute
                    // %s to be %s", propertyName,
                    // attribute.getName(), propertyValue));
                    m.invoke(attribute, propertyValue);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.warn(String.format("Failed to set property %s on attribute %s",
                            propertyName, attribute.getName()));
                }
            }
        } catch (Exception e) {
            log.error(
                    String.format("Failed to set properties on attribute %s", attribute.getName()),
                    e);
        }

    }

    public static void setPropertiesFromStrings(Attribute attribute,
            Map<String, String> properties) {
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
                        // log.info(String.format("Setting property %s on
                        // attribute %s to be %s", propertyName,
                        // attribute.getName(), properties.get(propertyName)));
                        m.invoke(attribute, properties.get(propertyName));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        log.warn(String.format("Failed to set property %s on attribute %s",
                                propertyName, attribute.getName()));
                    }
                }
            }
        } catch (Exception e) {
            log.error(
                    String.format("Failed to set properties on attribute %s", attribute.getName()),
                    e);
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

    public static String toJavaClass(String... dataTypes) {
        String javaClass = null;
        for (String dataType: dataTypes) {
            if (StringUtils.isNotBlank(dataType)) {
                switch (dataType.toLowerCase()) {
                    case "string":
                        javaClass = String.class.getSimpleName();
                        break;
                    case "int":
                        javaClass = Integer.class.getSimpleName();
                        break;
                    case "long":
                        javaClass = Long.class.getSimpleName();
                        break;
                    case "float":
                        javaClass = Float.class.getSimpleName();
                        break;
                    case "double":
                        javaClass = Double.class.getSimpleName();
                        break;
                    case "boolean":
                        javaClass = Boolean.class.getSimpleName();
                        break;
                    default:
                        log.warn("Cannot convert data-type " + dataType + " to java class.");
                }
            }
            if (StringUtils.isNotBlank(javaClass)) {
                break;
            }
        }
        return javaClass;
    }
}
