package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFileGeneratorStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayLaunchWorkflowDeploymentTestNG extends CDLWorkflowDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    @Inject
    private PlayProxy playProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    RecommendationService recommendationService;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.export.s3.bucket}")
    protected String exportS3Bucket;

    String randId = UUID.randomUUID().toString();

    private Play defaultPlay;
    private PlayLaunch defaultPlayLaunch;

    PlayLaunchConfig playLaunchConfig = null;

    DropBoxSummary dropboxSummary = null;

    @Override
    public Tenant currentTestTenant() {
        return testPlayCreationHelper.getTenant();
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String existingTenant = null;//"LETest1547165867101";
        Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName(), true);
        
        playLaunchConfig = new PlayLaunchConfig.Builder()
                .existingTenant(existingTenant)
                .mockRatingTable(false)
                .testPlayCrud(false)
                .destinationSystemType(CDLExternalSystemType.MAP)
                .destinationSystemId("Marketo_"+System.currentTimeMillis())
                .trayAuthenticationId(UUID.randomUUID().toString())
                .audienceId(UUID.randomUUID().toString())
                .topNCount(160L)
                .featureFlags(featureFlags)
                .build(); 

        testPlayCreationHelper.setupTenantAndCreatePlay(playLaunchConfig);
        super.deploymentTestBed = testPlayCreationHelper.getDeploymentTestBed();

        FeatureFlagValueMap ffVMap = super.deploymentTestBed.getFeatureFlags();
        log.info("Feature Flags for Tenant: " + ffVMap);
        Assert.assertTrue(ffVMap.containsKey(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName()));
        Assert.assertTrue(ffVMap.get(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName()));
        dropboxSummary = dropBoxProxy.getDropBox(currentTestTenant().getId());
        assertNotNull(dropboxSummary);
        log.info("Tenant DropboxSummary: {}", JsonUtils.serialize(dropboxSummary));
        assertNotNull(dropboxSummary.getDropBox());

        defaultPlay = testPlayCreationHelper.getPlay();
        defaultPlayLaunch = testPlayCreationHelper.getPlayLaunch();
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

    @Test(groups = "deployment", dependsOnMethods = "testPlayLaunchWorkflow")
    public void testVerifyAndCleanupUploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(playLaunchConfig.getDestinationSystemId());
        config.setDestinationSysType(playLaunchConfig.getDestinationSystemType());

        PlayLaunchExportFileGeneratorStep exportFileGen = new PlayLaunchExportFileGeneratorStep();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        StringBuilder sb = new StringBuilder(pathBuilder.getS3AtlasFileExportsDir(exportS3Bucket, dropboxFolderName));
        sb.append("/").append(exportFileGen.buildNamespace(config).replaceAll("\\.", "/"));
        String s3FolderPath = sb.substring(sb.indexOf(exportS3Bucket)+exportS3Bucket.length());

        log.info("Verifying S3 Folder Path " + s3FolderPath);
        // Get S3 Files for this PlayLaunch Config
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(exportS3Bucket, s3FolderPath);
        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), 2);
        assertTrue(s3Objects.get(0).getKey().contains("Recommendations"));

        boolean csvFileExists = false, jsonFileExists = false;
        for (S3ObjectSummary s3Obj : s3Objects) {
            if (s3Obj.getKey().contains(".csv")) {
                csvFileExists = true;
            }
            if (s3Obj.getKey().contains(".json")) {
                jsonFileExists = true;
            }
        }
        assertTrue(csvFileExists, "CSV file doesnot exists");
        assertTrue(jsonFileExists, "JSON file doesnot exists");

        log.info("Cleaning up S3 path " + s3FolderPath);
        try {
            s3Service.cleanupPrefix(exportS3Bucket, s3FolderPath);
            s3Service.cleanupPrefix(exportS3Bucket, dropboxFolderName);
        } catch (Exception ex) {
            log.error("Error while cleaning up dropbox files ", ex);
        }
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
    }

}
