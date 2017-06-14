package com.latticeengines.datacloud.etl.orchestration.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineTriggeredConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.PredefinedScheduleConfig;

@Component
public class OrchestrationEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(OrchestrationEntityMgrImplTestNG.class);

    @Autowired
    private OrchestrationEntityMgr orchestrationEntityMgr;

    private List<Orchestration> orchestrations = new ArrayList<>();
    // 

    // Name, ConfigStr, expected config class, expected pipeline length,
    // expected first step, expected next step
    @DataProvider(name = "Orchestrations")
    private static Object[][] getOrchestrations() {
        return new Object[][] { //
                { "TestOrchestration1",
                        "{\"ClassName\":\"PredefinedScheduleConfig\",\"EnginePipelineConfig\":\"[{\\\"Engine\\\":\\\"INGESTION\\\",\\\"Name\\\":\\\"DnBCacheSeed\\\"}]\"}",
                        PredefinedScheduleConfig.class, 1, Pair.of(DataCloudEngine.INGESTION, "DnBCacheSeed"), null }, //
                { "TestOrchestration2",
                        "{\"ClassName\":\"EngineTriggeredConfig\",\"EnginePipelineConfig\":\"[{\\\"Engine\\\":\\\"INGESTION\\\",\\\"Name\\\":\\\"DnBCacheSeed\\\"},{\\\"Engine\\\":\\\"TRANSFORMATION\\\",\\\"Name\\\":\\\"DnBCacheSeed\\\"}]\",\"Engine\":\"INGESTION\",\"EngineName\":\"DnBCacheSeed\",\"TriggerStrategy\":\"LATEST_VERSION\"}",
                        EngineTriggeredConfig.class, 2, Pair.of(DataCloudEngine.INGESTION, "DnBCacheSeed"),
                        Pair.of(DataCloudEngine.TRANSFORMATION, "DnBCacheSeed") }, //
        };
    }

    @Test(groups = "functional", priority = 1, dataProvider = "Orchestrations")
    public void init(String name, String config, Class<?> configCls, int pipelineLen,
            Pair<DataCloudEngine, String> firstStep, Pair<DataCloudEngine, String> nextStep) {
        Orchestration orchestration = new Orchestration();
        orchestration.setName(name);
        orchestration.setSchedularEnabled(false);
        orchestration.setMaxRetries(3);
        orchestration.setConfigStr(config);
        orchestrationEntityMgr.save(orchestration);
    }

    @Test(groups = "functional", priority = 2, dataProvider = "Orchestrations")
    public void testFindByName(String name, String configStr, Class<?> configCls, int pipelineLen,
            Pair<DataCloudEngine, String> firstStep, Pair<DataCloudEngine, String> nextStep) {
        Orchestration orch = orchestrationEntityMgr.findByField("Name", name);
        orchestrations.add(orch);
        OrchestrationConfig config = orch.getConfig();
        log.info("OrchestrationConfig: " + config.toString());
        Assert.assertTrue(configCls.isInstance(config));
        if (config instanceof PredefinedScheduleConfig) {
            Assert.assertNull(((PredefinedScheduleConfig) config).getCronExpression());
        }
        if (config instanceof EngineTriggeredConfig) {
            Assert.assertNotNull(((EngineTriggeredConfig) config).getEngine());
            Assert.assertNotNull(((EngineTriggeredConfig) config).getEngineName());
            Assert.assertNotNull(((EngineTriggeredConfig) config).getStrategy());
        }
        List<Pair<DataCloudEngine, String>> enginePipeline = config.getEnginePipeline();
        Assert.assertEquals(enginePipeline.size(), pipelineLen);
        Assert.assertEquals(config.firstStep(), firstStep);
        Assert.assertEquals(config.nextStep(config.firstStep()), nextStep);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        for (Orchestration orch : orchestrations) {
            orchestrationEntityMgr.delete(orch);
        }
    }
}
