package com.latticeengines.dataplatform.runtime.mapreduce;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.SamplingElement;

public class EventDataSamplingJobUnitTestNG {
    
    private String inputDir = null;
    private String outputDir = null;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL inputUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE_TEST");
        inputDir = inputUrl.getPath();
        outputDir = inputDir + "/samples";
        FileUtils.deleteDirectory(new File(outputDir));
    }

    @Test(groups = "unit")
    public void run() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(30);
        SamplingElement s2 = new SamplingElement();
        s2.setName("s2");
        s2.setPercentage(60);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(s2);
        
        
        int res = ToolRunner.run(new EventDataSamplingJob(), new String[] { inputDir, outputDir, samplingConfig.toString() });
        
        assertEquals(0, res);
        File outputDirFile = new File(outputDir);
        
        String[] avroFiles = outputDirFile.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".avro");
            }
            
        });

        assertEquals(4, avroFiles.length);
    }
}
