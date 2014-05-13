package com.latticeengines.perf.exposed.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.perf.exposed.metric.sink.SocketSink;

public class PerfFunctionalTestBaseUnitTestNG {
    
    private PerfFunctionalTestBase testBase;
    private MetricsRecord record = null;
    private SocketSink sink = null;
    
    @BeforeClass(groups = "unit")
    public void init() throws Exception {
        FileUtils.deleteQuietly(new File("/tmp/metricfile.txt"));
        Configuration conf = new PropertiesConfiguration("hadoop-metrics2.properties").interpolatedConfiguration();
        SubsetConfiguration subsetConfig = new SubsetConfiguration(conf, "ledpjob.sink.file", ".");
        sink = new SocketSink();
        sink.init(subsetConfig);
        record = mock(MetricsRecord.class);
        when(record.timestamp()).thenReturn(System.currentTimeMillis());
        when(record.context()).thenReturn("ledpjob");
        when(record.name()).thenReturn("metric1");
        List<MetricsTag> tags = new ArrayList<MetricsTag>();
        MetricsTag tag1 = mock(MetricsTag.class);
        when(tag1.name()).thenReturn("Queue");
        when(tag1.value()).thenReturn("Priority0.0");
        when(tag1.description()).thenReturn("Queue tag");
        tags.add(tag1);
        MetricsTag tag2 = mock(MetricsTag.class);
        when(tag2.name()).thenReturn("Priority");
        when(tag2.value()).thenReturn("0");
        when(tag2.description()).thenReturn("Priority tag");
        tags.add(tag2);
        when(record.tags()).thenReturn(tags);
        List<AbstractMetric> metrics = new ArrayList<AbstractMetric>();
        AbstractMetric metric = mock(AbstractMetric.class);
        metrics.add(metric);
        when(metric.name()).thenReturn("ContainerWaitTime");
        when(metric.value()).thenReturn(1234);
        when(record.metrics()).thenReturn(metrics);
        
        testBase = new PerfFunctionalTestBase("/tmp/metricfile.txt");
        testBase.beforeClass();
    }
    
    @Test(groups = "unit", dependsOnMethods = { "beforeMethod" })
    public void afterMethod() {
        testBase.afterMethod();
        assertFalse(sink.writeToFile());
    }

    @Test(groups = "unit")
    public void beforeMethod() throws Exception {
        testBase.beforeMethod();
        sink.putMetrics(record);
        testBase.flushToFile();
        String contents = FileUtils.readFileToString(new File("/tmp/metricfile.txt"));
        assertTrue(contents.contains("ledpjob.metric1:Queue=Priority0.0,Priority=0,ContainerWaitTime=1234"));
    }
    
    @Test(groups = "unit", dependsOnMethods = { "afterMethod" })
    public void afterClass() throws Exception {
        testBase.afterClass();
    }
    
}
