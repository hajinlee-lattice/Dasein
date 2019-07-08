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
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayLaunchWorkflowDeploymentTestNG extends CDLDeploymentTestNGBase {

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

    private PlayLaunchConfig marketoPlayLaunchConfig;

    private PlayLaunchConfig s3PlayLaunchConfig;

    private DropBoxSummary dropboxSummary = null;

    private Tenant currentTestTenant() {
        return testPlayCreationHelper.getTenant();
    }

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        String existingTenant = null;
        Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALPHA_FEATURE.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName(), true);

        marketoPlayLaunchConfig = new PlayLaunchConfig.Builder().existingTenant(existingTenant).mockRatingTable(false)
                .testPlayCrud(false).destinationSystemType(CDLExternalSystemType.MAP)
                .destinationSystemName(CDLExternalSystemName.Marketo)
                .destinationSystemId("Marketo_" + System.currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B)))
                .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                .topNCount(160L).featureFlags(featureFlags).build();

        s3PlayLaunchConfig = new PlayLaunchConfig.Builder().existingTenant(existingTenant).mockRatingTable(false)
                .testPlayCrud(false).destinationSystemType(CDLExternalSystemType.FILE_SYSTEM)
                .bucketsToLaunch(
                        new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B, RatingBucketName.C)))
                .destinationSystemName(CDLExternalSystemName.AWS_S3).destinationSystemId("Lattice_S3").topNCount(200L)
                .featureFlags(featureFlags).build();

        testPlayCreationHelper.setupTenantAndCreatePlay(marketoPlayLaunchConfig);
        super.testBed = testPlayCreationHelper.getDeploymentTestBed();
        setMainTestTenant(super.testBed.getMainTestTenant());
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

    @Test(groups = "deployment-app")
    public void testMarketoPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(marketoPlayLaunchConfig);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testMarketoPlayLaunchWorkflow")
    public void testVerifyAndCleanupMarketoUploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(marketoPlayLaunchConfig.getDestinationSystemId());
        config.setDestinationSysType(marketoPlayLaunchConfig.getDestinationSystemType());
        config.setDestinationSysName(marketoPlayLaunchConfig.getDestinationSystemName());

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

    @Test(groups = "deployment-app", dependsOnMethods = "testVerifyAndCleanupMarketoUploadedS3File")
    public void testS3LaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        testPlayCreationHelper.createPlayLaunch(s3PlayLaunchConfig);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(s3PlayLaunchConfig);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3LaunchWorkflow")
    public void testVerifyAndCleanupS3UploadedS3File() {
        String dropboxFolderName = dropboxSummary.getDropBox();

        // Create PlayLaunchExportFilesGeneratorConfiguration Config
        PlayLaunchExportFilesGeneratorConfiguration config = new PlayLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(s3PlayLaunchConfig.getDestinationSystemId());
        config.setDestinationSysType(s3PlayLaunchConfig.getDestinationSystemType());
        config.setDestinationSysName(s3PlayLaunchConfig.getDestinationSystemName());

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
}
