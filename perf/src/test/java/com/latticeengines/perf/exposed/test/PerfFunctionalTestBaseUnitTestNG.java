package com.latticeengines.perf.exposed.test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.perf.exposed.metric.sink.SwitchableFileSink;

public class PerfFunctionalTestBaseUnitTestNG {
    
    private PerfFunctionalTestBase testBase;
    private SwitchableFileSink sink;
    private SubsetConfiguration subsetConfig = null;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteQuietly(new File("ledpjob-metrics.out"));
        FileUtils.deleteDirectory(new File("/tmp/ledpjob"));
        setupSink();
        testBase = new PerfFunctionalTestBase("localhost", "test", "test");
    }
    
    private void setupSink() throws Exception {
        Configuration conf = new PropertiesConfiguration("hadoop-metrics2.properties").interpolatedConfiguration();
        subsetConfig = new SubsetConfiguration(conf, "ledpjob.sink.file", ".");
        sink = new SwitchableFileSink();
        
    }

    @Test(groups = "unit", dependsOnMethods = { "beforeClass" })
    public void afterClass() {
        testBase.afterClass();
        assertTrue(new File("/tmp/ledpjob/STOP").exists());
        assertTrue(new File("ledpjob-metrics.out").exists());
        assertFalse(sink.writeToFile());
    }

    @Test(groups = "unit")
    public void beforeClass() {
        testBase.beforeClass();
        sink.init(subsetConfig);
        assertTrue(new File("/tmp/ledpjob/START").exists());
        assertTrue(sink.writeToFile());
    }
    
}
