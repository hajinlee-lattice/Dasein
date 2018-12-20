package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class JoinScriptTestNG extends TestJoinTestNGBase {

    private ScriptJobConfig jobConfig;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        uploadInputAvro();
        Map<String, String> map = new HashMap<>();
        map.put("JOIN_KEY", "Field1");
        map.put("AGG_KEY", "Field2");

        jobConfig = new ScriptJobConfig();
        jobConfig.setParams(JsonUtils.convertValue(map, JsonNode.class));
        jobConfig.setNumTargets(2);
    }

    @Test(groups = "functional", dataProvider = "dataProvider")
    public void testWithData(SparkInterpreter interpreter) {
        String ext = SparkInterpreter.Scala.equals(interpreter) ? "scala" : "py";
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/join." + ext);
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(interpreter);

        SparkJobResult result = runSparkScript(script, jobConfig);
        verifyResult(result);
    }

    @DataProvider(name = "dataProvider")
    private Object[][] provideData() {
        return new Object[][] { //
                { SparkInterpreter.Scala }, //
                { SparkInterpreter.Python }
        };
    }

}
