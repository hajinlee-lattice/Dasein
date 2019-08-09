package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.TestPartitionTestNGBase;

public class PartitionScalaScriptTestNG extends TestPartitionTestNGBase {

    private ScriptJobConfig jobConfig;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
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

    @Test(groups = "functional",priority = 1)
    public void testScalaScript() {
        dataCnt = uploadInputAvro();
        setParamsWithPartition(true);
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/partition.scala");
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(SparkInterpreter.Scala);
        SparkJobResult result = runSparkScript(script, jobConfig);
        verifier = this::verifyOutput1;
        inputSources =result.getTargets();
        verifyResult(result);
    }

    @Test(groups = "functional",priority = 2)
    public void testPartitionScalaScript() {
        setParamsWithPartition(false);
        uploadOutputAsInput(inputSources);
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/partition.scala");
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(SparkInterpreter.Scala);
        SparkJobResult result  = runSparkScript(script, jobConfig);
        verifier = this::verifyOutput2;
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is Scala script output!");
    }


}
