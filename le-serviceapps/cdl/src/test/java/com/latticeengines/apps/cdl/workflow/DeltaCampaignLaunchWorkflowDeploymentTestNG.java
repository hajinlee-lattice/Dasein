package com.latticeengines.apps.cdl.workflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportFileGeneratorStep;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class DeltaCampaignLaunchWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchWorkflowDeploymentTestNG.class);

    @Inject
    private DeltaCampaignLaunchWorkflowSubmitter deltaCampaignWorkflowSubmitter;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    private TestPlaySetupConfig liverampTestPlaySetupConfig;
    private TestPlayChannelConfig liverampTestPlayChannelSetupConfig;

    private Play defaultPlay;
    private PlayLaunch defaultPlayLaunch;

    private DropBoxSummary dropboxSummary;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
        moveAvroFilesToHDFS();

        String existingTenant = null;
        Map<String, Boolean> featureFlags = new HashMap<>();

        String addContactsTable = setupLiveRampTable("/tmp/addLiveRampResult", "AddLiveRampContacts");
        String removeContactsTable = setupLiveRampTable("/tmp/removeLiveRampResult", "RemoveLiveRampContacts");

        liverampTestPlayChannelSetupConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.DSP).destinationSystemName(
                        CDLExternalSystemName.MediaMath)
                .destinationSystemId("LiveRamp_" + System
                        .currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B))).topNCount(160L)
                .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                .audienceType(AudienceType.ACCOUNTS).addContactsTable(addContactsTable)
                .removeContactsTable(removeContactsTable).build();

        liverampTestPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant)
                .mockRatingTable(false).testPlayCrud(false).addChannel(liverampTestPlayChannelSetupConfig)
                .featureFlags(featureFlags).existingTenant(testPlayCreationHelper.getCustomerSpace()).build();

        testPlayCreationHelper.setupTenantAndCreatePlay(liverampTestPlaySetupConfig);
        super.testBed = testPlayCreationHelper.getDeploymentTestBed();
        setMainTestTenant(super.testBed.getMainTestTenant());

        dropboxSummary = dropBoxProxy.getDropBox(testPlayCreationHelper.getCustomerSpace());
        assertNotNull(dropboxSummary);
        log.info("Tenant DropboxSummary: {}", JsonUtils.serialize(dropboxSummary));
        assertNotNull(dropboxSummary.getDropBox());

        defaultPlay = testPlayCreationHelper.getPlay();
        defaultPlayLaunch = testPlayCreationHelper.getPlayLaunch();

        defaultPlayLaunch.setPlay(defaultPlay);
    }

    @Test(groups = "deployment-app")
    public void testLiveRampPlayLaunchWorkflow() throws Exception {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        Long pid = deltaCampaignWorkflowSubmitter.submit(defaultPlayLaunch);
        assertNotNull(pid);
        log.info(String.format("PlayLaunch Workflow Pid is %s", pid));

        String applicationId = workflowProxy.getApplicationIdByWorkflowJobPid(testPlayCreationHelper.getCustomerSpace(),
                pid);

        JobStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testLiveRampPlayLaunchWorkflow")
    public void testVerifyAndCleanupLiveRampUploadedS3File() {
        DeltaCampaignLaunchExportFilesGeneratorConfiguration config = new DeltaCampaignLaunchExportFilesGeneratorConfiguration();
        config.setPlayName(defaultPlay.getName());
        config.setPlayLaunchId(defaultPlayLaunch.getId());
        config.setDestinationOrgId(liverampTestPlayChannelSetupConfig.getDestinationSystemId());
        config.setDestinationSysType(liverampTestPlayChannelSetupConfig.getDestinationSystemType());
        config.setDestinationSysName(liverampTestPlayChannelSetupConfig.getDestinationSystemName());

        DeltaCampaignLaunchExportFileGeneratorStep exportFileGen = new DeltaCampaignLaunchExportFileGeneratorStep();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        StringBuilder sb = new StringBuilder(pathBuilder.getS3AtlasFileExportsDir(exportS3Bucket, dropboxSummary.getDropBox()));
        sb.append("/").append(exportFileGen.buildNamespace(config).replaceAll("\\.", "/"));
        String s3FolderPath = sb.substring(sb.indexOf(exportS3Bucket) + exportS3Bucket.length());

        log.info("Verifying S3 Folder Path " + s3FolderPath);

        assertS3FileCount(s3FolderPath, 3);
        cleanupS3Files(s3FolderPath);
    }

    private void moveAvroFilesToHDFS() throws IOException {
        moveAvroFileToHDFS("campaign/liveramp/addLiverampBlock.avro", "/tmp/addLiveRampResult/addLiverampBlock.avro");
        moveAvroFileToHDFS("campaign/liveramp/removeLiverampBlock.avro",
                "/tmp/removeLiveRampResult/removeLiverampBlock.avro");
    }

    private void moveAvroFileToHDFS(String fromPath, String toPath) throws IOException {
        String toFolder = toPath.substring(0, toPath.lastIndexOf('/'));
        createDirsIfDoesntExist(toFolder);

        URL url = ClassLoader.getSystemResource(fromPath);
        File localFile = new File(url.getFile());
        log.info("Taking file from: " + localFile.getAbsolutePath());
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), toPath);

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, toPath));
        log.info("Added Match Block uploaded to: " + toPath);
    }

    private void createDirsIfDoesntExist(String dir) throws IOException {
        if (!HdfsUtils.isDirectory(yarnConfiguration, dir)) {
            log.info("Dir does not exist, creating dir: " + dir);
            HdfsUtils.mkdir(yarnConfiguration, dir);
        }
    }

    private String setupLiveRampTable(String filePath, String name) throws IOException {
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setPath(filePath);

        String tableName = createTableInMetadataProxy(name, ContactMasterConstants.TPS_ATTR_RECORD_ID, dataUnit);

        return tableName;
    }

    private String createTableInMetadataProxy(String tableName, String primaryKey, HdfsDataUnit jobTarget)
            throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(testPlayCreationHelper.getCustomerSpace());

        String tableAvroPath = PathBuilder
                .buildDataTablePath(podId, customerSpace)
                .toString();
        createDirsIfDoesntExist(tableAvroPath);

        Table table = SparkUtils.hdfsUnitToTable(tableName, primaryKey, jobTarget, yarnConfiguration,
                podId,
                customerSpace);
        metadataProxy.createTable(testPlayCreationHelper.getCustomerSpace(), table.getName(), table);

        log.info("Created " + tableName + " at " + table.getExtracts().get(0).getPath());

        return tableName;
    }

    private void assertS3FileCount(String s3FolderPath, int count) {
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(exportS3Bucket, s3FolderPath);

        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), count);
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
    }

    private void cleanupS3Files(String s3FolderPath) {
        String dropboxFolderName = dropboxSummary.getDropBox();

        log.info("Cleaning up S3 path " + s3FolderPath);
        try {
            s3Service.cleanupDirectory(exportS3Bucket, s3FolderPath);
            s3Service.cleanupDirectory(exportS3Bucket, dropboxFolderName);
        } catch (Exception ex) {
            log.error("Error while cleaning up dropbox files ", ex);
        }
    }

    @Override
    public void testWorkflow() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void verifyTest() {
        // TODO Auto-generated method stub

    }
}
