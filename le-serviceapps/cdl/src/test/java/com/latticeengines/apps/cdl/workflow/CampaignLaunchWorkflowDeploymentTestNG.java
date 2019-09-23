package com.latticeengines.apps.cdl.workflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFileGeneratorStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class CampaignLaunchWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    @Value("${aws.customer.s3.bucket}")
    private String customerS3Bucket;

    String randId = UUID.randomUUID().toString();

    private Play defaultPlay;
    private PlayLaunch defaultPlayLaunch;

    private TestPlaySetupConfig marketoTestPlaySetupConfig;
    private TestPlayChannelConfig marketoTestPlayChannelSetupConfig;

    private TestPlaySetupConfig s3TestPlaySetupConfig;
    private TestPlayChannelConfig s3TestPlayChannelSetupConfig;

    private TestPlaySetupConfig linkedInTestPlaySetupConfig;
    private TestPlayChannelConfig linkedInTestPlayChannelSetupConfig;

    private TestPlaySetupConfig facebookTestPlaySetupConfig;
    private TestPlayChannelConfig facebookTestPlayChannelSetupConfig;

    private DropBoxSummary dropboxSummary = null;

    private Tenant currentTestTenant() {
        return testPlayCreationHelper.getTenant();
    }

    @BeforeClass(groups = "deployment-app", enabled = true)
    public void setup() throws Exception {
        String existingTenant = null;
        Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALPHA_FEATURE.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName(), true);

        marketoTestPlayChannelSetupConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.MAP).destinationSystemName(CDLExternalSystemName.Marketo)
                .destinationSystemId("Marketo_" + System.currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B))).topNCount(160L)
                .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString()).build();

        marketoTestPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant)
                .mockRatingTable(true).testPlayCrud(false).addChannel(marketoTestPlayChannelSetupConfig)
                .featureFlags(featureFlags).build();

        s3TestPlayChannelSetupConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.FILE_SYSTEM)
                .bucketsToLaunch(
                        new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B, RatingBucketName.C)))
                .destinationSystemName(CDLExternalSystemName.AWS_S3).destinationSystemId("Lattice_S3").topNCount(200L)
                .build();

        s3TestPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant).mockRatingTable(false)
                .testPlayCrud(false).addChannel(s3TestPlayChannelSetupConfig).featureFlags(featureFlags).build();

        linkedInTestPlayChannelSetupConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.ADS).destinationSystemName(CDLExternalSystemName.LinkedIn)
                .destinationSystemId("LinkedIn_" + System.currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B))).topNCount(160L)
                .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                .audienceType(AudienceType.ACCOUNTS).build();

        linkedInTestPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant)
                .mockRatingTable(false).testPlayCrud(false).addChannel(linkedInTestPlayChannelSetupConfig)
                .featureFlags(featureFlags).build();

        testPlayCreationHelper.setupTenantAndCreatePlay(marketoTestPlaySetupConfig);
        super.testBed = testPlayCreationHelper.getDeploymentTestBed();
        setMainTestTenant(super.testBed.getMainTestTenant());
        checkpointService.setMainTestTenant(super.testBed.getMainTestTenant());
        // checkpointService.resumeCheckpoint( //
        // "update3", //
        // CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);

        FeatureFlagValueMap ffVMap = super.testBed.getFeatureFlags();
        log.info("Feature Flags for Tenant: " + ffVMap);
        Assert.assertTrue(ffVMap.containsKey(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName()));
        Assert.assertTrue(ffVMap.get(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName()));
        dropboxSummary = dropBoxProxy.getDropBox(currentTestTenant().getId());
        assertNotNull(dropboxSummary);
        log.info("Tenant DropboxSummary: {}", JsonUtils.serialize(dropboxSummary));
        assertNotNull(dropboxSummary.getDropBox());

        defaultPlay = testPlayCreationHelper.getPlay();
    }

    @Test(groups = "deployment-app", enabled = true)
    public void testMarketoPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(marketoTestPlaySetupConfig, true);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testMarketoPlayLaunchWorkflow", enabled = false)
    public void testVerifyAndCleanupMarketoUploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(marketoTestPlayChannelSetupConfig.getDestinationSystemId());
        config.setDestinationSysType(marketoTestPlayChannelSetupConfig.getDestinationSystemType());
        config.setDestinationSysName(marketoTestPlayChannelSetupConfig.getDestinationSystemName());

        PlayLaunchExportFileGeneratorStep exportFileGen = new PlayLaunchExportFileGeneratorStep();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        StringBuilder sb = new StringBuilder(pathBuilder.getS3AtlasFileExportsDir(exportS3Bucket, dropboxFolderName));
        sb.append("/").append(exportFileGen.buildNamespace(config).replaceAll("\\.", "/"));
        String s3FolderPath = sb.substring(sb.indexOf(exportS3Bucket) + exportS3Bucket.length());

        log.info("Verifying S3 Folder Path " + s3FolderPath);
        // Get S3 Files for this PlayLaunch Config
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(exportS3Bucket, s3FolderPath);
        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), 2);
        assertTrue(s3Objects.get(0).getKey().contains("Recommendations"));
        // 426 rows

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

    @Test(groups = "deployment-app", dependsOnMethods = "testVerifyAndCleanupMarketoUploadedS3File", enabled = false)
    public void testS3LaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        testPlayCreationHelper.createPlayLaunch(s3TestPlaySetupConfig);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(s3TestPlaySetupConfig, true);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3LaunchWorkflow", enabled = false)
    public void testVerifyAndCleanupS3UploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(s3TestPlayChannelSetupConfig.getDestinationSystemId());
        config.setDestinationSysType(s3TestPlayChannelSetupConfig.getDestinationSystemType());
        config.setDestinationSysName(s3TestPlayChannelSetupConfig.getDestinationSystemName());

        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        String s3FolderPath = pathBuilder.getS3CampaignExportDir(customerS3Bucket, dropboxFolderName)
                .replace(pathBuilder.getProtocol() + pathBuilder.getProtocolSeparator() + pathBuilder.getPathSeparator()
                        + customerS3Bucket + pathBuilder.getPathSeparator(), "");

        log.info("Verifying S3 Folder Path " + s3FolderPath);
        // Get S3 Files for this PlayLaunch Config
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(customerS3Bucket, s3FolderPath);
        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), 2);
        assertTrue(s3Objects.get(0).getKey().contains(defaultPlay.getDisplayName()));
        assertTrue(s3Objects.get(0).getKey().contains(defaultPlay.getName()));
        // 392 rows
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
            s3Service.cleanupPrefix(customerS3Bucket, s3FolderPath);
            s3Service.cleanupPrefix(customerS3Bucket, dropboxFolderName);
        } catch (Exception ex) {
            log.error("Error while cleaning up dropbox files ", ex);
        }
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testVerifyAndCleanupS3UploadedS3File", enabled = false)
    public void testLinkedInPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(linkedInTestPlaySetupConfig);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testLinkedInPlayLaunchWorkflow", enabled = false)
    public void testVerifyAndCleanupLinkedInUploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(linkedInTestPlayChannelSetupConfig.getDestinationSystemId());
        config.setDestinationSysType(linkedInTestPlayChannelSetupConfig.getDestinationSystemType());
        config.setDestinationSysName(linkedInTestPlayChannelSetupConfig.getDestinationSystemName());

        PlayLaunchExportFileGeneratorStep exportFileGen = new PlayLaunchExportFileGeneratorStep();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        StringBuilder sb = new StringBuilder(pathBuilder.getS3AtlasFileExportsDir(exportS3Bucket, dropboxFolderName));
        sb.append("/").append(exportFileGen.buildNamespace(config).replaceAll("\\.", "/"));
        String s3FolderPath = sb.substring(sb.indexOf(exportS3Bucket) + exportS3Bucket.length());

        log.info("Verifying S3 Folder Path " + s3FolderPath);
        // Get S3 Files for this PlayLaunch Config
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(exportS3Bucket, s3FolderPath);
        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), 2);
        assertTrue(s3Objects.get(0).getKey().contains("Recommendations"));
        // 426 rows

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

    @Override
    public void testWorkflow() {
        // Unused
    }

    @Override
    protected void verifyTest() {
        // // Unused

    }
}
