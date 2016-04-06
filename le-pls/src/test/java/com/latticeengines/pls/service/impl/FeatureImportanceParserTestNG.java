package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class FeatureImportanceParserTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private Configuration yarnConfiguration = new Configuration();
    private String defaultFs = null;
    private String inputFile = null;
    private String dir = null;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/featureimportanceparser/inputfile.txt");
        inputFile = url.getFile();
        
        URL rfModelUrl = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/featureimportanceparser/rf_model.txt");
        
        dir = String.format("/user/s-analytics/customers/%s/models/T1/%s/1459652427726_0080", //
                "Tenant1.Tenant1.Production", "4de7e20a-f8d9-4d91-8c25-62541cd201d9");
        
        HdfsUtils.rmdir(yarnConfiguration, dir);
        createRFModelFile(rfModelUrl.getPath());
        
        defaultFs = yarnConfiguration.get("fs.defaultFS").split("//")[1];
    }
    
    private void createRFModelFile(String rfModelPath) throws Exception {
        HdfsUtils.mkdir(yarnConfiguration, dir);    
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfModelPath, dir);
    }
    
    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        //FileUtils.deleteQuietly(new File("outputfile.txt"));
        HdfsUtils.rmdir(yarnConfiguration, dir);
    }
    

    @Test(groups = "unit")
    public void main() throws Exception {
        FeatureImportanceParser.main(new String[] { //
                "--inputfile", //
                inputFile, //
                "--outputfile", //
                "outputfile.txt", //
                "--hdfs-hostport", //
                defaultFs //
        });
        assertTrue(new File("outputfile.txt").exists());
        assertTrue(new File("outputfile.txt").getTotalSpace() > 0);
    }
}
