package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayLaunchWorkflowDeploymentTestNG extends CDLWorkflowDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    @Autowired
    private PlayProxy playProxy;

    @Autowired
    RecommendationService recommendationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    String randId = UUID.randomUUID().toString();

    private Play defaultPlay;

    private PlayLaunch defaultPlayLaunch;

    PlayLaunchConfig playLaunchConfig = null;

    @Override
    public Tenant currentTestTenant() {
        return testPlayCreationHelper.getTenant();
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String existingTenant = null;//"LETest1547165867101";
        playLaunchConfig = new PlayLaunchConfig.Builder()
                .existingTenant(existingTenant)
                .mockRatingTable(false)
                .testPlayCrud(false)
                .destinationSystemType(CDLExternalSystemType.MAP)
                .destinationSystemId("Marketo_"+System.currentTimeMillis())
                .topNCount(160L)
                .build(); 

        testPlayCreationHelper.setupTenantAndCreatePlay(playLaunchConfig);

        super.deploymentTestBed = testPlayCreationHelper.getDeploymentTestBed();
        
        defaultPlay = testPlayCreationHelper.getPlay();
        defaultPlayLaunch = testPlayCreationHelper.getPlayLaunch();
    }

    //@Test(groups = "deployment")
    public void testYarnConfiguration() {
        String recAvroHdfsFilePath = "/Pods/Default/Contracts/LETest1547168631346/Tenants/LETest1547168631346/Spaces/Production/Data/Tables/1547168906968/avro/launch__ccb0e070-3907-4b4a-b4f0-ba31fc52480c.avro";

        Function<GenericRecord, GenericRecord> RecommendationJsonFormatter = new Function<GenericRecord, GenericRecord>() {
            @Override
            public GenericRecord apply(GenericRecord rec) {
                Object obj = rec.get("CONTACTS");
                if (obj != null && StringUtils.isNotBlank(obj.toString())) {
                    obj = JsonUtils.deserialize(obj.toString(), new TypeReference<List<Map<String, String>>>() {
                    });
                    rec.put("CONTACTS", obj);
                }
                return rec;
            }
        };
        try {
            File localJsonFile = Files.createTempFile("tempfile", ".json").toFile();
            log.info("Generating JSON File: {}", localJsonFile);
            FileSystem fs = new Path(recAvroHdfsFilePath).getFileSystem(yarnConfiguration);
            log.info("FileSystem: " + JsonUtils.serialize(yarnConfiguration));
            log.info("FileSystem: " + fs.getCanonicalServiceName() + " "+fs.getName() + " " + fs.getScheme() + " " + fs.getHomeDirectory() + " " + fs.getUri() + " " + JsonUtils.serialize(fs.getConf())) ;
            AvroUtils.convertAvroToJSON(yarnConfiguration, new Path(recAvroHdfsFilePath), localJsonFile, RecommendationJsonFormatter);
        } catch(Exception e) {
            
        }
    }

    @Test(groups = "deployment")
    public void testPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(playLaunchConfig);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        
    }

}
