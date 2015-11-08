package com.latticeengines.scoring.runtime.mapreduce;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

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

}
