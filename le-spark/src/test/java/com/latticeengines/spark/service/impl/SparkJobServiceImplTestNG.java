package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SparkJobServiceImplTestNG extends SparkJobFunctionalTestNGBase {

    @Inject
    private SparkJobService sparkJobService;

    private ScriptJobConfig jobConfig;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        Map<String, Object> map = new HashMap<>();
        map.put("NUM_SAMPLES", 10000);
        ObjectMapper om = new ObjectMapper();
        JsonNode params = om.valueToTree(map);
        jobConfig = new ScriptJobConfig();
        jobConfig.setParams(params);
        jobConfig.setNumTargets(0);
        jobConfig.setWorkspace(getWorkspace());
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void tearDown() {
        tearDownLivyEnvironment();
    }

    @Test(groups = "functional")
    public void testPythonScript() {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/calc_pi.py");
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(SparkInterpreter.Python);
        String output = sparkJobService.runScript(session, script, jobConfig).getOutput();
        Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
    }

    @Test(groups = "functional")
    public void testScalaScript() {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/calc_pi.scala");
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(SparkInterpreter.Scala);
        String output = sparkJobService.runScript(session, script, jobConfig).getOutput();
        Assert.assertTrue(output.startsWith("Pi is roughly 3.1"), output);
    }

}
