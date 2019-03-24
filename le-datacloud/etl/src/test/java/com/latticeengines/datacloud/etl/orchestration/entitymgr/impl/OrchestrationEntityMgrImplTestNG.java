package com.latticeengines.datacloud.etl.orchestration.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;
import com.latticeengines.domain.exposed.datacloud.orchestration.ExternalTriggerConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.PredefinedScheduleConfig;

@Component
public class OrchestrationEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrchestrationEntityMgrImplTestNG.class);

    @Autowired
    private OrchestrationEntityMgr orchestrationEntityMgr;

    private List<Orchestration> orchestrations = new ArrayList<>();

    // Name, ConfigStr, expected config class, expected pipeline length,
    // expected first step, expected next step
    @DataProvider(name = "Orchestrations")
    private static Object[][] getOrchestrations() {
        return new Object[][] { //
                { "TestOrchestration1",
                        "{\"ClassName\":\"PredefinedScheduleConfig\",\"PipelineConfig\":[{\"Engine\":\"INGESTION\",\"EngineName\":\"DnBCacheSeed\",\"Timeout\":0}]}",
                        PredefinedScheduleConfig.class, 1,
                        new DataCloudEngineStage(DataCloudEngine.INGESTION, "DnBCacheSeed", 0), null }, //
                { "TestOrchestration2",
                        "{\"ClassName\":\"ExternalTriggerConfig\",\"PipelineConfig\":[{\"Engine\":\"INGESTION\",\"EngineName\":\"DnBCacheSeed\",\"Timeout\":0},{\"Engine\":\"TRANSFORMATION\",\"EngineName\":\"DnBCacheSeed\",\"Timeout\":0}],\"Engine\":\"INGESTION\",\"EngineName\":\"DnBCacheSeed\",\"TriggerStrategy\":\"LATEST_VERSION\"}",
                        ExternalTriggerConfig.class, 2,
                        new DataCloudEngineStage(DataCloudEngine.INGESTION, "DnBCacheSeed", 0),
                        new DataCloudEngineStage(DataCloudEngine.TRANSFORMATION, "DnBCacheSeed", 0) }, //
        };
    }

    @Test(groups = "functional", priority = 1, dataProvider = "Orchestrations")
    public void init(String name, String config, Class<?> configCls, int pipelineLen,
            DataCloudEngineStage firstStep, DataCloudEngineStage nextStep) {
        Orchestration orch = new Orchestration();
        orch.setName(name);
        orch.setSchedularEnabled(false);
        orch.setMaxRetries(3);
        orch.setConfigStr(config);
        orchestrationEntityMgr.save(orch);
    }

    @Test(groups = "functional", priority = 2, dataProvider = "Orchestrations")
    public void testFindByName(String name, String configStr, Class<?> configCls, int pipelineLen,
            DataCloudEngineStage firstStep, DataCloudEngineStage nextStep) {
        Orchestration orch = orchestrationEntityMgr.findByField("Name", name);
        orchestrations.add(orch);
        OrchestrationConfig config = orch.getConfig();
        log.info("OrchestrationConfig: " + config.toString());
        Assert.assertTrue(configCls.isInstance(config));
        if (config instanceof PredefinedScheduleConfig) {
            Assert.assertNull(((PredefinedScheduleConfig) config).getCronExpression());
        }
        if (config instanceof ExternalTriggerConfig) {
            Assert.assertNotNull(((ExternalTriggerConfig) config).getEngine());
            Assert.assertNotNull(((ExternalTriggerConfig) config).getEngineName());
            Assert.assertNotNull(((ExternalTriggerConfig) config).getStrategy());
        }
        List<DataCloudEngineStage> enginePipeline = config.getPipeline();
        Assert.assertEquals(enginePipeline.size(), pipelineLen);
        Assert.assertEquals(config.firstStage(), firstStep);
        Assert.assertEquals(config.nextStage(config.firstStage()), nextStep);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        for (Orchestration orch : orchestrations) {
            orchestrationEntityMgr.delete(orch);
        }
    }
}
