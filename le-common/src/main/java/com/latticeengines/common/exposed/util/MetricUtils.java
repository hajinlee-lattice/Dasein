package com.latticeengines.common.exposed.util;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;

public class MetricUtils {

    private static final Log log = LogFactory.getLog(MetricUtils.class);


    public static Map<String, String> toStringMap(Map<MetricTag.Tag, String> tagMap) {
        if (tagMap == null) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<MetricTag.Tag, String> entry : tagMap.entrySet()) {
            map.put(entry.getKey().getTagName(), entry.getValue());
        }
        return map;
    }

    public static Map<MetricTag.Tag, String> parseTags(Dimension dimension) {
        Map<MetricTag.Tag, String> tagMap = new HashMap<>();
        for (Method method : dimension.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricTag.class)) {
                Map.Entry<MetricTag.Tag, String> entry = parseTag(dimension, method);
                if (entry != null) {
                    tagMap.put(entry.getKey(), entry.getValue());
                }
            } else if (method.isAnnotationPresent(MetricTagGroup.class)) {
                tagMap.putAll(parseTagGroup(dimension, method));
            }
        }
        return tagMap;
    }

    public static Map<String, Object> parseFields(Fact fact) {
        Map<String, Object> fieldMap = new HashMap<>();
        for (Method method : fact.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricField.class)) {
                Map.Entry<String, Object> entry = parseField(fact, method);
                if (entry != null) {
                    fieldMap.put(entry.getKey(), entry.getValue());
                }
            } else if (method.isAnnotationPresent(MetricFieldGroup.class)) {
                fieldMap.putAll(parseFieldGroup(fact, method));
            }
        }
        return fieldMap;
    }

    public static String toLogMessage(Measurement<?, ?> measurement) {
        Dimension dimension = measurement.getDimension();
        Map<MetricTag.Tag, String> tagMap = parseTags(dimension);

        Fact fact = measurement.getFact();
        Map<String, Object> fieldMap = parseFields(fact);

        List<String> tokens = new ArrayList<>(Collections.singleton("Measurement=" + measurement.getName()));
        for (Map.Entry<MetricTag.Tag, String> entry : tagMap.entrySet()) {
            tokens.add(String.format("%s=%s", entry.getKey().getTagName(), entry.getValue()));
        }
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            if (entry.getValue() instanceof String) {
                tokens.add(String.format("%s=%s", entry.getKey(), "\"" + entry.getValue() + "\""));
            } else {
                tokens.add(String.format("%s=%s", entry.getKey(), entry.getValue().toString()));
            }
        }

        return StringUtils.join(tokens, ", ");
    }

    @VisibleForTesting
    static Map.Entry<MetricTag.Tag, String> parseTag(Dimension dimension, Method method) {
        try {
            if (String.class.isAssignableFrom(method.getReturnType())) {
                MetricTag metricMetricTag = method.getAnnotation(MetricTag.class);
                MetricTag.Tag tag = metricMetricTag.tag();
                method.setAccessible(true);
                String value = (String) method.invoke(dimension);
                if (MetricTag.Tag.providedByFramework().contains(tag)) {
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

    @VisibleForTesting
    static Map<MetricTag.Tag, String> parseTagGroup(Dimension dimension, Method method) {
        try {
            Map<MetricTag.Tag, String> tagMap = new HashMap<>();
            if (Dimension.class.isAssignableFrom(method.getReturnType())) {
                MetricTagGroup metricTagGroup = method.getAnnotation(MetricTagGroup.class);
                MetricTag.Tag[] includes = metricTagGroup.includes();
                MetricTag.Tag[] excludes = metricTagGroup.excludes();

                Set<MetricTag.Tag> includeSet = new HashSet<>(Arrays.asList(includes));
                Set<MetricTag.Tag> excludeSet = new HashSet<>(Arrays.asList(excludes));

                boolean includeAll = includes.length == 0;

                method.setAccessible(true);
                Dimension tagGroup = (Dimension) method.invoke(dimension);
                for (Map.Entry<MetricTag.Tag, String> entry : parseTags(tagGroup).entrySet()) {
                    MetricTag.Tag tag = entry.getKey();
                    if ((includeAll || includeSet.contains(tag)) && !excludeSet.contains(tag)) {
                        tagMap.put(entry.getKey(), entry.getValue());
                    }
                }

                return tagMap;

            } else {
                throw new RuntimeException("MetricTagGroup must be an implementation of Dimension.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricTagGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + dimension, e);
        }
    }

    @VisibleForTesting
    static Map.Entry<String, Object> parseField(Fact Fact, Method method) {
        try {
            MetricField metricField = method.getAnnotation(MetricField.class);
            MetricField.FieldType fieldType = metricField.fieldType();
            if (fieldType.getJavaClass().isAssignableFrom(method.getReturnType())) {
                String key = metricField.name();
                method.setAccessible(true);
                Object value = method.invoke(Fact);
                return new AbstractMap.SimpleEntry<>(key, value);
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
    static Map<String, Object> parseFieldGroup(Fact fact, Method method) {
        try {
            Map<String, Object> fieldMap = new HashMap<>();
            if (Fact.class.isAssignableFrom(method.getReturnType())) {
                MetricFieldGroup metricFieldGroup = method.getAnnotation(MetricFieldGroup.class);
                String[] includes = metricFieldGroup.includes();
                String[] excludes = metricFieldGroup.excludes();

                Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
                Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

                boolean includeAll = includes.length == 0;

                method.setAccessible(true);
                Fact fieldGroup = (Fact) method.invoke(fact);
                for (Map.Entry<String, Object> entry : parseFields(fieldGroup).entrySet()) {
                    String fieldName = entry.getKey();
                    if ((includeAll || includeSet.contains(fieldName)) && !excludeSet.contains(fieldName)) {
                        fieldMap.put(entry.getKey(), entry.getValue());
                    }
                }

                return fieldMap;
            } else {
                throw new RuntimeException(MetricFieldGroup.class.getSimpleName() + " must be an implementation of "
                        + Fact.class.getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + MetricFieldGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + fact, e);
        }
    }

    public static void scan(Measurement<?, ?> measurement) {
        System.out.println("Measurement: " + measurement.getName() + "\n");

        System.out.println("Scanning tags ... ");
        Set<MetricTag.Tag> tags = scanTags(measurement.getDimension().getClass());
        Set<String> tagNames = new HashSet<>();
        for (MetricTag.Tag tag : tags) {
            tagNames.add("[" + tag.getTagName() + "]");
        }
        System.out.println("Final set of tags are: " + StringUtils.join(tagNames, ", ") + "\n");

        System.out.println("Scanning fields ... ");
        Set<String> fieldNames = scanFields(measurement.getFact().getClass());
        System.out.println("Fields are: " + StringUtils.join(fieldNames, ", "));
    }

    public static Set<MetricTag.Tag> scanTags(Class<?> dimensionClass) {
        Set<MetricTag.Tag> tagSet = new HashSet<>();
        for (Method method : dimensionClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(MetricTag.class)) {
                MetricTag.Tag tag = scanTag(method);
                if (tag != null) {
                    tagSet.add(tag);
                    System.out.println(
                            String.format(" Find tag [%s] in [%s]", tag.getTagName(), dimensionClass.getSimpleName()));
                }
            } else if (method.isAnnotationPresent(MetricTagGroup.class)) {
                Set<MetricTag.Tag> newTags = scanTagGroup(dimensionClass, method);
                for (MetricTag.Tag tag : newTags) {
                    if (tagSet.contains(tag)) {
                        throw new RuntimeException("Duplicated tag " + tag.getTagName());
                    }
                    tagSet.add(tag);
                    System.out.println(String.format(" Add tag [%s] in [%s] to [%s]", tag.getTagName(),
                            method.getReturnType().getSimpleName(), dimensionClass.getSimpleName()));
                }
            }
        }
        return tagSet;
    }

    private static Set<MetricTag.Tag> scanTagGroup(Class<?> dimensionClass, Method method) {
        try {
            Set<MetricTag.Tag> tagSet = new HashSet<>();
            if (Dimension.class.isAssignableFrom(method.getReturnType())) {
                MetricTagGroup metricTagGroup = method.getAnnotation(MetricTagGroup.class);
                MetricTag.Tag[] includes = metricTagGroup.includes();
                MetricTag.Tag[] excludes = metricTagGroup.excludes();

                Set<MetricTag.Tag> includeSet = new HashSet<>(Arrays.asList(includes));
                Set<MetricTag.Tag> excludeSet = new HashSet<>(Arrays.asList(excludes));

                boolean includeAll = includes.length == 0;

                Class<?> tagGroupClass = method.getReturnType();
                for (MetricTag.Tag tag : scanTags(tagGroupClass)) {
                    if ((includeAll || includeSet.contains(tag)) && !excludeSet.contains(tag)) {
                        tagSet.add(tag);
                    } else {
                        System.out.println(String.format(" Exclude tag [%s] in [%s] from [%s]", tag.getTagName(),
                                tagGroupClass.getSimpleName(), dimensionClass.getSimpleName()));
                    }
                }

                return tagSet;

            } else {
                throw new RuntimeException("MetricTagGroup must be an implementation of Dimension.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan " + MetricTagGroup.class.getSimpleName() + " from method "
                    + method.getName() + " in " + dimensionClass.getSimpleName(), e);
        }
    }

    private static MetricTag.Tag scanTag(Method method) {
        try {
            if (String.class.isAssignableFrom(method.getReturnType())) {
                MetricTag metricMetricTag = method.getAnnotation(MetricTag.class);
                MetricTag.Tag tag = metricMetricTag.tag();
                if (MetricTag.Tag.providedByFramework().contains(tag)) {
                    log.warn(tag + " is ignored, as it will be provided by the framework.");
                    return null;
                } else {
                    return tag;
                }

            } else {
                throw new RuntimeException("MetricTag can only be applied Strings.");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to scan " + MetricTag.class.getSimpleName() + " from method " + method.getName(), e);
        }
    }

    public static Set<String> scanFields(Class<?> factClass) {
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
        return fieldSet;
    }

    private static Set<String> scanFieldGroup(Class<?> factClass, Method method) {
        try {
            Set<String> fieldSet = new HashSet<>();
            if (Fact.class.isAssignableFrom(method.getReturnType())) {
                MetricFieldGroup metricFieldGroup = method.getAnnotation(MetricFieldGroup.class);
                String[] includes = metricFieldGroup.includes();
                String[] excludes = metricFieldGroup.excludes();

                Set<String> includeSet = new HashSet<>(Arrays.asList(includes));
                Set<String> excludeSet = new HashSet<>(Arrays.asList(excludes));

                boolean includeAll = includes.length == 0;

                Class<?> fieldGroupClass = method.getReturnType();
                for (String fieldName : scanFields(fieldGroupClass)) {
                    if ((includeAll || includeSet.contains(fieldName)) && !excludeSet.contains(fieldName)) {
                        fieldSet.add(fieldName);
                    } else {
                        System.out.println(String.format(" Exclude field [%s] in [%s] from [%s]", fieldName,
                                fieldGroupClass.getSimpleName(), factClass.getSimpleName()));
                    }
                }

                return fieldSet;
            } else {
                throw new RuntimeException(MetricFieldGroup.class.getSimpleName() + " must be an implementation of "
                        + Fact.class.getSimpleName());
            }
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
