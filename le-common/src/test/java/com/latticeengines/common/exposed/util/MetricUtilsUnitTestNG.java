package com.latticeengines.common.exposed.util;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;

public class MetricUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testMetricTag() throws NoSuchMethodException {
        Class<SimpleTestClass> clz = SimpleTestClass.class;
        Method method = clz.getDeclaredMethod("getTenantId");
        Assert.assertNotNull(method);
        Assert.assertTrue(method.isAnnotationPresent(MetricTag.class));

        MetricTag metricTag = method.getAnnotation(MetricTag.class);
        Assert.assertEquals(metricTag.tag(), "Tag1");

        SimpleTestClass instance = new SimpleTestClass();
        Map.Entry<String, String> entry = MetricUtils.parseTag(instance, method);
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("Tag1", null), entry);

        instance.setTenantId("tenant1");
        entry = MetricUtils.parseTag(instance, method);
        Assert.assertEquals("tenant1", entry.getValue());

        Map<String, String> tagSet = MetricUtils.parseTags(instance);
        Assert.assertEquals(tagSet.size(), 2);

        Set<String> tags = MetricUtils.scanTags(SimpleTestClass.class);
        Assert.assertEquals(tags.size(), 2);
    }

    @Test(groups = "unit")
    public void testMetricTagGroup() throws NoSuchMethodException {
        Class<ComplexTestClass> clz = ComplexTestClass.class;
        Method method = clz.getDeclaredMethod("getTagGroup");
        Assert.assertNotNull(method);
        Assert.assertTrue(method.isAnnotationPresent(MetricTagGroup.class));

        ComplexTestClass instance = new ComplexTestClass();
        SimpleTestClass tagGroup = new SimpleTestClass();
        tagGroup.setTenantId("tenant1");
        tagGroup.setSourceName("source1");
        instance.setTagGroup(tagGroup);

        Map<String, String> tagSet = MetricUtils.parseTagGroup(instance, method);
        Assert.assertEquals(tagSet.size(), 1);

        tagSet = MetricUtils.parseTags(instance);
        Assert.assertEquals(tagSet.size(), 1);
    }

    @Test(groups = "unit")
    public void testMetricField() throws NoSuchMethodException {
        Class<SimpleTestClass> clz = SimpleTestClass.class;
        Method method = clz.getDeclaredMethod("getField");
        Assert.assertNotNull(method);
        Assert.assertTrue(method.isAnnotationPresent(MetricField.class));

        MetricField metricField = method.getAnnotation(MetricField.class);
        Assert.assertEquals(metricField.name(), "field");
        Assert.assertEquals(metricField.fieldType(), MetricField.FieldType.INTEGER);

        SimpleTestClass instance = new SimpleTestClass();
        Map.Entry<String, Object> entry = MetricUtils.parseField(instance, method);
        Assert.assertEquals(new AbstractMap.SimpleEntry<>("field", null), entry);

        instance.setField(23);
        entry = MetricUtils.parseField(instance, method);
        Assert.assertEquals(23, entry.getValue());

        Map<String, Object> fieldSet = MetricUtils.parseFields(instance);
        Assert.assertEquals(fieldSet.size(), 4);

        Set<String> fields = MetricUtils.scanFields(SimpleTestClass.class);
        Assert.assertEquals(fields.size(), 4);
    }

    @Test(groups = "unit")
    public void testMetricFieldGroup() throws NoSuchMethodException {
        Class<ComplexTestClass> clz = ComplexTestClass.class;
        Method method = clz.getDeclaredMethod("getTagGroup");
        Assert.assertNotNull(method);
        Assert.assertTrue(method.isAnnotationPresent(MetricFieldGroup.class));

        ComplexTestClass instance = new ComplexTestClass();
        SimpleTestClass fieldGroup = new SimpleTestClass();
        fieldGroup.setBooleanField(true);
        fieldGroup.setField(12);
        fieldGroup.setStringField("yes");
        fieldGroup.setDoubleField(2.0);
        instance.setTagGroup(fieldGroup);

        Map<String, Object> fieldMap = MetricUtils.parseFieldGroup(instance, method);
        Assert.assertEquals(fieldMap.size(), 2);

        fieldMap = MetricUtils.parseFields(instance);
        Assert.assertEquals(fieldMap.size(), 2);
    }

    @Test(groups = "unit")
    public void testScanMeasurement() {
        MetricUtils.scan(TestMeasurement.class);
    }

    @Test(groups = "unit")
    public void testToLogMessage() {
        System.out.println(MetricUtils.toLogMessage(new TestMeasurement()));
    }

    private class SimpleTestClass implements Dimension, Fact {
        private String tenantId;
        private String sourceName;
        private Integer field;
        private Boolean booleanField;
        private String stringField;
        private Double doubleField;

        @MetricTag(tag = "Tag1")
        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        @MetricTag(tag = "Tag2")
        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        @MetricField(name = "field", fieldType = MetricField.FieldType.INTEGER)
        public Integer getField() {
            return field;
        }

        public void setField(Integer field) {
            this.field = field;
        }

        @MetricField(name = "booleanField", fieldType = MetricField.FieldType.BOOLEAN)
        public Boolean getBooleanField() {
            return booleanField;
        }

        public void setBooleanField(Boolean booleanField) {
            this.booleanField = booleanField;
        }

        @MetricField(name = "stringField", fieldType = MetricField.FieldType.STRING)
        public String getStringField() {
            return stringField;
        }

        public void setStringField(String stringField) {
            this.stringField = stringField;
        }

        @MetricField(name = "doubleField", fieldType = MetricField.FieldType.DOUBLE)
        public Double getDoubleField() {
            return doubleField;
        }

        public void setDoubleField(Double doubleField) {
            this.doubleField = doubleField;
        }
    }

    private class ComplexTestClass implements Dimension, Fact {
        private SimpleTestClass tagGroup;

        @MetricTagGroup(excludes = { "Tag2" })
        @MetricFieldGroup(includes = { "field", "doubleField", "booleanField" }, excludes = { "doubleField" })
        public SimpleTestClass getTagGroup() {
            return tagGroup;
        }

        public void setTagGroup(SimpleTestClass tagGroup) {
            this.tagGroup = tagGroup;
        }
    }

    private class TestMeasurement implements Measurement<SimpleTestClass, ComplexTestClass> {

        public ComplexTestClass getDimension() {
            ComplexTestClass instance = new ComplexTestClass();
            SimpleTestClass tagGroup = new SimpleTestClass();
            tagGroup.setTenantId("tenant1");
            tagGroup.setSourceName("source1");
            instance.setTagGroup(tagGroup);
            return instance;
        }

        public SimpleTestClass getFact() {
            SimpleTestClass fieldGroup = new SimpleTestClass();
            fieldGroup.setBooleanField(true);
            fieldGroup.setField(12);
            fieldGroup.setStringField("yes");
            fieldGroup.setDoubleField(2.0);
            return fieldGroup;
        }

        public RetentionPolicy getRetentionPolicy() {
            return new RetentionPolicy() {
                @Override
                public String getDuration() {
                    return null;
                }

                @Override
                public Integer getReplication() {
                    return null;
                }

                @Override
                public String getName() {
                    return "default";
                }
            };
        }

    }

}
