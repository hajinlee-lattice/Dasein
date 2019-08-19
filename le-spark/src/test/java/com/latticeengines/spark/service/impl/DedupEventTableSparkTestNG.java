package com.latticeengines.spark.service.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.InputStreamSparkScript;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkInterpreter;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.TestDedupEventTableTestNGBase;

public class DedupEventTableSparkTestNG extends TestDedupEventTableTestNGBase {

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        uploadInputAvro();
    }

    @Test(groups = "functional", dataProvider = "dataProvider")
    public void testWithData(SparkInterpreter interpreter, DataUnit.DataFormat dataFormat) {
        String ext = SparkInterpreter.Scala.equals(interpreter) ? "scala" : "py";
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("scripts/dedupEventTable." + ext);
        InputStreamSparkScript script = new InputStreamSparkScript();
        script.setStream(is);
        script.setInterpreter(interpreter);

        ScriptJobConfig jobConfig = getJobConfig(dataFormat);
        SparkJobResult result = runSparkScript(script, jobConfig);
        verifyResult(result);
    }

    @DataProvider(name = "dataProvider")
    private Object[][] provideData() {
        return new Object[][] { //
                { SparkInterpreter.Scala, null } };
    }

    private ScriptJobConfig getJobConfig(DataUnit.DataFormat dataFormat) {
        Map<String, String> map = new HashMap<>();
        map.put("EVENT", "Event");
        map.put("LID_FIELD", "LatticeAccountId");
        map.put("INT_LDC_REMOVED", "__LDC_Removed__");
        map.put("INT_LDC_DEDUPE_ID", "__LDC_DedupeId__");
        map.put("INT_LDC_LID", "__LDC_LID__");

        ScriptJobConfig jobConfig = new ScriptJobConfig();
        jobConfig.setParams(JsonUtils.convertValue(map, JsonNode.class));
        jobConfig.setNumTargets(1);
        jobConfig.setSpecialTarget(0, dataFormat);
        return jobConfig;
    }

}
