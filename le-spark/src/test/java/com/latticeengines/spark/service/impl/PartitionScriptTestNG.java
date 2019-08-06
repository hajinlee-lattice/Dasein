package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
//import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.spark.testframework.TestPartitionTestNGBase;

public class PartitionScriptTestNG extends TestPartitionTestNGBase {
    @Inject
    private SparkJobService sparkJobService;

    private ScriptJobConfig jobConfig;

    private List<HdfsDataUnit> inputSources;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        uploadInputAvro();
        jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(1);
        jobConfig.setWorkspace(getWorkspace());
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() {
        tearDownLivyEnvironment();
    }

    private void setParamsWithPartition(boolean isPartition){
        Map<String, Object> map = new HashMap<>();
        map.put("Partition", isPartition);
        ObjectMapper om = new ObjectMapper();
        JsonNode params = om.valueToTree(map);
        jobConfig.setParams(params);
    }

//    @Test(groups = "functional",priority = 1)
//    public void testPythonScript() {
//        setParamsWithPartition(true);
//        InputStream is = Thread.currentThread().getContextClassLoader() //
//                .getResourceAsStream("scripts/partition.py");
//        InputStreamSparkScript script = new InputStreamSparkScript();
//        script.setStream(is);
//        script.setInterpreter(SparkInterpreter.Python);
//        runSparkScript(script, jobConfig);
//        inputSources =jobConfig.getTargets();
////        Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
//    }
//
//    @Test(groups = "functional",priority = 2)
//    public void testPartitionPythonScript() {
//        setParamsWithPartition(false);
//        uploadOutputAsInput(inputSources,"Field1");
//        InputStream is = Thread.currentThread().getContextClassLoader() //
//                .getResourceAsStream("scripts/partition.py");
//        InputStreamSparkScript script = new InputStreamSparkScript();
//        script.setStream(is);
//        script.setInterpreter(SparkInterpreter.Python);
//        String output = runSparkScript(script, jobConfig).getOutput();
//        System.out.println("Output:"+output);
//        //Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
//    }



    @Test(groups = "functional",priority = 1)
    public void testScalaScript() {
        setParamsWithPartition(true);
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/partition.scala");
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(SparkInterpreter.Scala);
        runSparkScript(script, jobConfig);
        inputSources =jobConfig.getTargets();
        //Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
    }

//    @Test(groups = "functional",priority = 2)
//    public void testPartitionScalaScript() {
//        setParamsWithPartition(false);
//        uploadOutputAsInput(inputSources,"Field1");
//        InputStream is = Thread.currentThread().getContextClassLoader() //
//                .getResourceAsStream("scripts/partition.scala");
//        InputStreamSparkScript script = new InputStreamSparkScript();
//        script.setStream(is);
//        script.setInterpreter(SparkInterpreter.Scala);
//        String output = runSparkScript(script, jobConfig).getOutput();
//        System.out.println("Output:"+output);
//        //Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
//    }
}
