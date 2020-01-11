package com.latticeengines.common.exposed.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;

public class MetricUtils {

    private static final Logger log = LoggerFactory.getLogger(MetricUtils.class);
    public static final String TAG_ENVIRONMENT = "Environment";
    public static final String TAG_HOST = "Host";
    public static final String TAG_STACK = "Stack";
    public static final String TAG_SERVICE = "Service";
    public static final String NULL = "null";
    public static Collection<String> frameworkTags = Arrays.asList(TAG_ENVIRONMENT, TAG_HOST, TAG_STACK);

    public static Map<String, String> parseTags(Dimension dimension) {
        return parseTagsInternal(dimension);
    }

    private static Map<String, String> parseTagsInternal(Object dimension) {
        Map<String, String> tagMap = new HashMap<>();
        if (dimension == null) {
            return tagMap;
        }
        for (Method method : dimension.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricTag.class)) {
                Map.Entry<String, String> entry = parseTag(dimension, method);
                if (entry != null) {
                    if (StringUtils.isNotEmpty(entry.getValue())) {
                        tagMap.put(entry.getKey(), entry.getValue());
                    } else {
                        tagMap.put(entry.getKey(), NULL);
                    }

                }
            } else if (method.isAnnotationPresent(MetricTagGroup.class)) {
                for (Map.Entry<String, String> entry : parseTagGroup(dimension, method).entrySet()) {
                    if (StringUtils.isNotEmpty(entry.getValue())) {
                        tagMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return tagMap;
    }

    public static Map<String, Object> parseFields(Fact fact) {
        return parseFieldsInternal(fact);
    }

    private static Map<String, Object> parseFieldsInternal(Object fact) {
        Map<String, Object> fieldMap = new HashMap<>();
        if (fact == null) {
            return fieldMap;
        }
        for (Method method : fact.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricField.class)) {
                Map.Entry<String, Object> entry = parseField(fact, method);
                if (entry != null) {
                    if (entry.getValue() != null && !((entry.getValue() instanceof String)
                            && StringUtils.isEmpty((String) entry.getValue()))) {
                        fieldMap.put(entry.getKey(), entry.getValue());
                    }
                }
            } else if (method.isAnnotationPresent(MetricFieldGroup.class)) {
                for (Map.Entry<String, Object> entry : parseFieldGroup(fact, method).entrySet()) {
                    if (entry.getValue() != null && !((entry.getValue() instanceof String)
                            && StringUtils.isEmpty((String) entry.getValue()))) {
                        fieldMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return fieldMap;
    }

    public static String toLogMessage(Measurement<?, ?> measurement) {
        return toLogMessage(measurement, null);
    }

    public static String toLogMessage(Measurement<?, ?> measurement, Map<String, Object> fieldMapArg) {
        Dimension dimension = measurement.getDimension();
        Map<String, String> tagMap = parseTags(dimension);

        Fact fact = measurement.getFact();

        Map<String, Object> fieldMap = fieldMapArg;
        if (fieldMap == null || fieldMap.isEmpty()) {
            fieldMap = parseFields(fact);
        }

        List<String> tokens = new ArrayList<>(
                Collections.singleton("Measurement=" + measurement.getClass().getSimpleName()));

        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            if (entry.getValue() instanceof String) {
                tokens.add(String.format("%s=%s", entry.getKey(), "\"" + entry.getValue() + "\""));
            } else {
                tokens.add(String.format("%s=%s", entry.getKey(), entry.getValue().toString()));
            }
        }

        for (Map.Entry<String, String> entry : tagMap.entrySet()) {
            tokens.add(String.format("%s=\"%s\"", entry.getKey(), entry.getValue()));
        }

        return StringUtils.join(tokens, " ");
    }

    @VisibleForTesting
    static Map.Entry<String, String> parseTag(Object dimension, Method method) {
        try {
            if (String.class.isAssignableFrom(method.getReturnType())) {
                MetricTag metricMetricTag = method.getAnnotation(MetricTag.class);
                String tag = metricMetricTag.tag();
                if (tag.isEmpty() && StringUtils.isNotEmpty(metricMetricTag.tagReferencingField())) {
                    Field field = dimension.getClass().getDeclaredField(metricMetricTag.tagReferencingField());
                    field.setAccessible(true);
                    tag = String.valueOf(field.get(dimension));
                }
                method.setAccessible(true);
                String value = (String) method.invoke(dimension);
                if (frameworkTags.contains(tag)) {
                    log.warn(tag + " is ignored, as it will be provided by the framework.");
                    return null;
                } else {
                    return new AbstractMap.SimpleEntry<>(tag, value);
                }

            } else {
                throw new RuntimeException("MetricTag can only be applied Strings.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricTag.class.getSimpleName() + " from method "
                    + method.getName() + " in " + dimension, e);
        }
    }

    @SuppressWarnings("rawtypes")
    @VisibleForTesting
    static Map<String, String> parseTagGroup(Object dimension, Method method) {
        try {
            Map<String, String> tagMap = new HashMap<>();
            MetricTagGroup metricTagGroup = method.getAnnotation(MetricTagGroup.class);
            String[] includes = metricTagGroup.includes();
            String[] excludes = metricTagGroup.excludes();

            Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
            Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

            boolean includeAll = includes.length == 0;

            method.setAccessible(true);
            Object tagGroup = method.invoke(dimension);

            if (tagGroup instanceof Collection) {
                for (Object obj : (Collection) tagGroup) {
                    for (Map.Entry<String, String> entry : parseTagsInternal(obj).entrySet()) {
                        String tag = entry.getKey();
                        if ((includeAll || includeSet.contains(tag)) && !excludeSet.contains(tag)) {
                            tagMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                }
            } else {
                for (Map.Entry<String, String> entry : parseTagsInternal(tagGroup).entrySet()) {
                    String tag = entry.getKey();
                    if ((includeAll || includeSet.contains(tag)) && !excludeSet.contains(tag)) {
                        tagMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            return tagMap;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricTagGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + dimension, e);
        }
    }

    @VisibleForTesting
    static Map.Entry<String, Object> parseField(Object Fact, Method method) {
        try {
            MetricField metricField = method.getAnnotation(MetricField.class);
            MetricField.FieldType fieldType = metricField.fieldType();
            if (fieldType.getJavaClass().isAssignableFrom(method.getReturnType())) {
                String key = metricField.name();
                method.setAccessible(true);
                Object value = method.invoke(Fact);
                if (value != null) {
                    Object type = fieldType.getJavaClass().cast(value);
                    return new AbstractMap.SimpleEntry<>(key, type);
                } else {
                    return new AbstractMap.SimpleEntry<>(key, null);
                }
            } else {
                throw new RuntimeException("The annotated type " + fieldType.getJavaClass().getSimpleName()
                        + " does not match the true return type " + method.getReturnType().getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricField.class.getSimpleName() + " from method "
                    + method.getName() + " in " + Fact, e);
        }
    }

    @VisibleForTesting
    static Map<String, Object> parseFieldGroup(Object fact, Method method) {
        try {
            Map<String, Object> fieldMap = new HashMap<>();
            MetricFieldGroup metricFieldGroup = method.getAnnotation(MetricFieldGroup.class);
            String[] includes = metricFieldGroup.includes();
            String[] excludes = metricFieldGroup.excludes();

            Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
            Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

            boolean includeAll = includes.length == 0;

            method.setAccessible(true);
            Object fieldGroup = method.invoke(fact);
            for (Map.Entry<String, Object> entry : parseFieldsInternal(fieldGroup).entrySet()) {
                String fieldName = entry.getKey();
                if ((includeAll || includeSet.contains(fieldName)) && !excludeSet.contains(fieldName)) {
                    fieldMap.put(entry.getKey(), entry.getValue());
                }
            }

            return fieldMap;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricFieldGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + fact, e);
        }
    }

    public static void scan(Class<? extends Measurement<?, ?>> measurementClass) {
        System.out.println("========================================\nScan Measurement: "
                + measurementClass.getSimpleName() + "\n========================================\n");
        for (Method method : measurementClass.getDeclaredMethods()) {
            if (method.getName().contains("getDimension")) {
                scanTags(method.getReturnType(), true, true);
            } else if (method.getName().contains("getFact")) {
                scanFields(method.getReturnType());
            }
        }
    }

    public static Set<String> scanTags(Class<?> dimensionClass) {
        return scanTags(dimensionClass, true, false);
    }

    private static Set<String> scanTags(Class<?> dimensionClass, boolean topLevel, boolean includeFrameworkTags) {
        if (topLevel && !dimensionClass.isInterface()) {
            System.out.println("Scan tags in Dimension: " + dimensionClass.getSimpleName()
                    + "\n----------------------------------------");
        }

        Set<String> tagSet = new HashSet<>();
        for (Method method : dimensionClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricTag.class)) {
                String tag = scanTag(method);
                if (tag != null) {
                    tagSet.add(tag);
                    System.out.println(String.format(" Find tag [%s] in [%s]", tag, dimensionClass.getSimpleName()));
                }
            } else if (method.isAnnotationPresent(MetricTagGroup.class)) {
                Set<String> newTags = scanTagGroup(dimensionClass, method);
                for (String tag : newTags) {
                    if (tagSet.contains(tag)) {
                        throw new RuntimeException("Duplicated tag " + tag);
                    }
                    tagSet.add(tag);
                    System.out.println(String.format(" Add tag [%s] in [%s] to [%s]", tag,
                            method.getReturnType().getSimpleName(), dimensionClass.getSimpleName()));
                }
            }
        }

        if (topLevel && !dimensionClass.isInterface()) {
            Set<String> tagNames = new HashSet<>();
            if (includeFrameworkTags) {
                for (String tag : frameworkTags) {
                    tagNames.add("[" + tag + "]");
                }
            }
            for (String tag : tagSet) {
                tagNames.add("[" + tag + "]");
            }
            System.out.println("----------------------------------------\nFinal set of tags are: "
                    + StringUtils.join(tagNames, ", ") + "\n");
        }

        return tagSet;
    }

    private static Set<String> scanTagGroup(Class<?> dimensionClass, Method method) {
        try {
            Set<String> tagSet = new HashSet<>();
            MetricTagGroup metricTagGroup = method.getAnnotation(MetricTagGroup.class);
            String[] includes = metricTagGroup.includes();
            String[] excludes = metricTagGroup.excludes();

            Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
            Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

            boolean includeAll = includes.length == 0;

            Class<?> tagGroupClass = method.getReturnType();
            for (String tag : scanTags(tagGroupClass, false, false)) {
                if ((includeAll || includeSet.contains(tag)) && !excludeSet.contains(tag)) {
                    tagSet.add(tag);
                } else {
                    System.out.println(String.format(" Exclude tag [%s] in [%s] from [%s]", tag,
                            tagGroupClass.getSimpleName(), dimensionClass.getSimpleName()));
                }
            }

            return tagSet;
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan " + MetricTagGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + dimensionClass.getSimpleName(), e);
        }
    }

    private static String scanTag(Method method) {
        try {
            if (String.class.isAssignableFrom(method.getReturnType())) {
                MetricTag metricMetricTag = method.getAnnotation(MetricTag.class);
                String tag = metricMetricTag.tag();
                if (frameworkTags.contains(tag)) {
                    log.warn(tag + " is ignored, as it will be provided by the framework.");
                    return null;
                } else {
                    return tag;
                }

            } else {
                throw new RuntimeException(
                        "MetricTag can only be applied to String. But this method returns " + method.getReturnType());
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to scan " + MetricTag.class.getSimpleName() + " from method " + method.getName(), e);
        }
    }

    public static Set<String> scanFields(Class<?> factClass) {
        return scanFields(factClass, true);
    }

    private static Set<String> scanFields(Class<?> factClass, boolean topLevel) {
        if (topLevel && !factClass.isInterface()) {
            System.out.println(
                    "Scan fields in Fact: " + factClass.getSimpleName() + "\n----------------------------------------");
        }
        Set<String> fieldSet = new HashSet<>();
        for (Method method : factClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricField.class)) {
                String field = scanField(method);
                if (field != null) {
                    fieldSet.add(field);
                    System.out.println(String.format(" Find field [%s] in [%s]", field, factClass.getSimpleName()));
                }
            } else if (method.isAnnotationPresent(MetricFieldGroup.class)) {
                Set<String> newFields = scanFieldGroup(factClass, method);
                for (String field : newFields) {
                    if (fieldSet.contains(field)) {
                        throw new RuntimeException("Duplicated field " + field);
                    }
                    fieldSet.add(field);
                    System.out.println(String.format(" Add field [%s] in [%s] to [%s]", field,
                            method.getReturnType().getSimpleName(), factClass.getSimpleName()));
                }
            }
        }

        if (topLevel && !factClass.isInterface()) {
            Set<String> fieldNames = new HashSet<>();
            for (String field : fieldSet) {
                fieldNames.add("[" + field + "]");
            }
            System.out.println("----------------------------------------\nFinal set of fields are: "
                    + StringUtils.join(fieldNames, ", ") + "\n");
        }

        return fieldSet;
    }

    private static Set<String> scanFieldGroup(Class<?> factClass, Method method) {
        try {
            Set<String> fieldSet = new HashSet<>();
            MetricFieldGroup metricFieldGroup = method.getAnnotation(MetricFieldGroup.class);
            String[] includes = metricFieldGroup.includes();
            String[] excludes = metricFieldGroup.excludes();

            Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
            Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

            boolean includeAll = includes.length == 0;

            Class<?> fieldGroupClass = method.getReturnType();
            for (String fieldName : scanFields(fieldGroupClass, false)) {
                if ((includeAll || includeSet.contains(fieldName)) && !excludeSet.contains(fieldName)) {
                    fieldSet.add(fieldName);
                } else {
                    System.out.println(String.format(" Exclude field [%s] in [%s] from [%s]", fieldName,
                            fieldGroupClass.getSimpleName(), factClass.getSimpleName()));
                }
            }

            return fieldSet;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricFieldGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + factClass.getSimpleName(), e);
        }
    }

    private static String scanField(Method method) {
        try {
            MetricField metricField = method.getAnnotation(MetricField.class);
            MetricField.FieldType fieldType = metricField.fieldType();
            if (fieldType.getJavaClass().isAssignableFrom(method.getReturnType())) {
                return metricField.name();
            } else {
                throw new RuntimeException("The annotated type " + fieldType.getJavaClass().getSimpleName()
                        + " does not match the true return type " + method.getReturnType().getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse " + MetricField.class.getSimpleName() + " from method " + method.getName(), e);
        }
    }

}
