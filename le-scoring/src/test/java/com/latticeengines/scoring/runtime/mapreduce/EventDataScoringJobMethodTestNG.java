package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

import static org.testng.Assert.assertEquals;

public class EventDataScoringJobMethodTestNG extends ScoringFunctionalTestNGBase{

    @Autowired
    private EventDataScoringJob scoringJob;

    @Autowired
    private Configuration hadoopConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    private String dir;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
        URL modelSummaryUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/data/part-m-00000.avro"); //
        HdfsUtils.mkdir(yarnConfiguration, dir);
        String filePath = dir + "/part-m-00000.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        dir = customerBaseDir + "/test_customer/scoring/data/some_table";
        HdfsUtils.rmdir(hadoopConfiguration, dir);
    }

    @Test(groups = "functional")
    public void testGenerateDataTypeSchema() throws JsonParseException, JsonMappingException, IOException, Exception{
        Schema schema = AvroUtils.getSchema(hadoopConfiguration, new Path(dir + "/part-m-00000.avro"));
        scoringJob.generateDataTypeSchema(schema, dir + "/datatype.json", hadoopConfiguration);
        @SuppressWarnings("deprecation")
        Map<String, String> map = new ObjectMapper().readValue(HdfsUtils.getHdfsFileContents(hadoopConfiguration, dir + "/datatype.json"), TypeFactory.mapType(HashMap.class, String.class, String.class));
        assertEquals(map.get("LeadID"), "1");
        assertEquals(map.get("Probability"), "0");
        assertEquals(map.get("RawScore"), "0");
        assertEquals(map.get("Play_Display_Name"), "1");
        assertEquals(map.get("Bucket_Display_Name"), "1");
        assertEquals(map.get("Score"), "0");
        assertEquals(map.get("Percentile"), "0");
        assertEquals(map.get("Lift"), "0");
    }
}
