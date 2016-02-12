package com.latticeengines.monitor.metric.service.impl;

import java.util.Collections;
import java.util.List;

import org.influxdb.InfluxDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
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
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-monitor-metric-context.xml" })
public class MetricServiceInfluxDbImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private MetricServiceInfluxDbImpl metricService;

    @Test(groups = "functional")
    public void testMetricServiceInitialization() throws Exception {
        if (!metricService.isEnabled()) {
            return;
        }

        InfluxDB influxDB = metricService.getInfluxDB();
        Assert.assertNotNull(influxDB);

        List<String> dbNames = metricService.listDatabases();
        for (MetricDB db : MetricDB.values()) {
            Assert.assertTrue(dbNames.contains(db.getDbName()), "MetricDB [" + db.getDbName() + "] does not exists.");
            List<String> policies = metricService.listRetentionPolicies(db.getDbName());
            for (RetentionPolicyImpl policy : RetentionPolicyImpl.values()) {
                Assert.assertTrue(policies.contains(policy.getName()), "MetricDB [" + db.getDbName()
                        + "] does not have registered retention policy [" + policy.getName() + "]");
            }
        }
    }

    @Test(groups = "functional")
    public void testWriteMetrics() throws Exception {
        if (!metricService.isEnabled()) {
            return;
        }
        TestMeasurement measurement = new TestMeasurement();
        metricService.write(MetricDB.TEST_DB,
                Collections.<Measurement<SimpleTestClass, ComplexTestClass>> singletonList(measurement));

        Thread.sleep(3000L);
    }

    private class SimpleTestClass implements Dimension, Fact {
        private String tenantId;
        private String sourceName;
        private Integer field;
        private Boolean booleanField;
        private String stringField;
        private Double doubleField;

        @MetricTag(tag = MetricTag.Tag.TENANT_ID)
        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        @MetricTag(tag = MetricTag.Tag.LDC_SOURCE_NAME)
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

        @MetricTagGroup(excludes = { MetricTag.Tag.LDC_SOURCE_NAME })
        @MetricFieldGroup(includes = { "field", "doubleField", "booleanField" }, excludes = { "doubleField" })
        public SimpleTestClass getTagGroup() {
            return tagGroup;
        }

        public void setTagGroup(SimpleTestClass tagGroup) {
            this.tagGroup = tagGroup;
        }
    }

    private class TestMeasurement implements Measurement<SimpleTestClass, ComplexTestClass> {

        public String getName() {
            return "TestMeasurement";
        }

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
            return RetentionPolicyImpl.ONE_HOUR;
        }
    }
}
