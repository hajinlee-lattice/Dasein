package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.TestPartitionTestNGBase;

public class PartitionScriptTestNG extends TestPartitionTestNGBase {

    private ScriptJobConfig jobConfig;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        dataCnt = uploadInputAvro();
        jobConfig = new ScriptJobConfig();
        jobConfig.setNumTargets(1);
        jobConfig.setWorkspace(getWorkspace());
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() {
        tearDownLivyEnvironment();
    }

    private void setParamsWithPartition(boolean isPartition) {
        Map<String, Object> map = new HashMap<>();
        map.put("Partition", isPartition);
        ObjectMapper om = new ObjectMapper();
        JsonNode params = om.valueToTree(map);
        jobConfig.setParams(params);
    }

    @Test(groups = "functional", dataProvider = "interpreter")
    public void testScript(SparkInterpreter interpreter) {
        String ext = SparkInterpreter.Scala.equals(interpreter) ? "scala" : "py";
        setParamsWithPartition(true);
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/partition." + ext);
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(interpreter);
        SparkJobResult result = runSparkScript(script, jobConfig);
        verifier = this::verifyOutput1;
        inputSources = result.getTargets();
        verifyResult(result);
    }

    @Test(groups = "functional", dataProvider = "interpreter", dependsOnMethods = "testScript")
    public void testPartitionScript(SparkInterpreter interpreter) {
        String ext = SparkInterpreter.Scala.equals(interpreter) ? "scala" : "py";
        setParamsWithPartition(false);
        copyOutputAsInput(inputSources);
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/partition." + ext);
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(interpreter);
        SparkJobResult result = runSparkScript(script, jobConfig);
        verifier = this::verifyOutput2;
        verifyResult(result);
    }

    @DataProvider(name = "interpreter")
    private Object[] provideData() {
        return new Object[]{SparkInterpreter.Scala, SparkInterpreter.Python};
    }

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is script output!");
    }

}
