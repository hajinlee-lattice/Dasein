package com.latticeengines.apps.cdl.workflow;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
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
import com.latticeengines.cdl.workflow.steps.play.LiveRampCampaignLaunchInitStep;
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

import au.com.bytecode.opencsv.CSVReader;

public class DeltaCampaignLaunchWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchWorkflowDeploymentTestNG.class);

    @Inject
    @Spy
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
    private TestPlayChannelConfig testPerformancePlayChannelConfig;
    private TestPlayChannelConfig liverampTestPlayChannelSetupConfig;

    private Play defaultPlay;
    private PlayLaunch defaultPlayLaunch;

    private DropBoxSummary dropboxSummary;

    private static final List<String> LIVERAMP_COL_NAME = Arrays
            .asList(LiveRampCampaignLaunchInitStep.RECORD_ID_DISPLAY_NAME);

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(false).when(deltaCampaignWorkflowSubmitter).enableExternalLaunch(any(), any());
        testPlayCreationHelper.setupTenantAndData();
        moveAvroFilesToHDFS();
        String addContactsTable = setupLiveRampTable("/tmp/addLiveRampResult", "AddLiveRampContacts");
        String removeContactsTable = setupLiveRampTable("/tmp/removeLiveRampResult", "RemoveLiveRampContacts");
        testPerformancePlayChannelConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.FILE_SYSTEM).destinationSystemName(
                        CDLExternalSystemName.AWS_S3).destinationSystemId("AWS_S3_" + System.currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B)))
                .audienceId(UUID.randomUUID().toString())
                .audienceType(AudienceType.CONTACTS).build();
        liverampTestPlayChannelSetupConfig = new TestPlayChannelConfig.Builder()
                .destinationSystemType(CDLExternalSystemType.DSP).destinationSystemName(
                        CDLExternalSystemName.MediaMath)
                .destinationSystemId("LiveRamp_" + System.currentTimeMillis())
                .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B))).topNCount(160L)
                .audienceId(UUID.randomUUID().toString())
                .audienceType(AudienceType.ACCOUNTS).addContactsTable(addContactsTable)
                .removeContactsTable(removeContactsTable).build();
        liverampTestPlaySetupConfig = new TestPlaySetupConfig.Builder().mockRatingTable(false).testPlayCrud(false)
                .addChannel(liverampTestPlayChannelSetupConfig)
                .existingTenant(testPlayCreationHelper.getCustomerSpace()).build();
        testPlayCreationHelper.setupTenantAndCreatePlay(liverampTestPlaySetupConfig);
        testBed = testPlayCreationHelper.getDeploymentTestBed();
        setMainTestTenant(testBed.getMainTestTenant());
        dropboxSummary = dropBoxProxy.getDropBox(testPlayCreationHelper.getCustomerSpace());
        assertNotNull(dropboxSummary);
        log.info("Tenant DropboxSummary: {}", JsonUtils.serialize(dropboxSummary));
        assertNotNull(dropboxSummary.getDropBox());
        defaultPlay = testPlayCreationHelper.getPlay();
        defaultPlayLaunch = testPlayCreationHelper.getPlayLaunch();
        defaultPlayLaunch.setPlay(defaultPlay);
    }

    @Override
    @Test(groups = "deployment-app")
    public void testWorkflow() throws Exception {
        testLiveRampPlayLaunchWorkflow();
        verifyTest();
    }

    @Override
    protected void verifyTest() {
        testVerifyAndCleanupLiveRampUploadedS3File();
    }

    public void testLiveRampPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        // TODO: Check the playlaunchchannelID instead of null
        Long pid = deltaCampaignWorkflowSubmitter.submit(defaultPlayLaunch, null);
        assertNotNull(pid);
        log.info(String.format("PlayLaunch Workflow Pid is %s", pid));
        String applicationId = workflowProxy.getApplicationIdByWorkflowJobPid(testPlayCreationHelper.getCustomerSpace(),
                pid);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

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
        // TODO: Fix the buildNamespace(), doesn't exist
        // sb.append("/").append(exportFileGen.buildNamespace(config).replaceAll("\\.",
        // "/"));
        String s3FolderPath = sb.substring(sb.indexOf(exportS3Bucket) + exportS3Bucket.length());
        log.info("Verifying S3 Folder Path " + s3FolderPath);
        List<String> s3CsvObjectKeys = assertS3FileCountAndGetS3CsvObj(s3FolderPath, 2);
        assertS3CsvContents(s3CsvObjectKeys, LIVERAMP_COL_NAME, 22, 15);
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

    private List<String> assertS3FileCountAndGetS3CsvObj(String s3FolderPath, int fileCount) {
        List<String> s3CsvObjectKeys = new ArrayList<String>();
        List<S3ObjectSummary> s3Objects = s3Service.listObjects(exportS3Bucket, s3FolderPath);
        assertNotNull(s3Objects);
        assertEquals(s3Objects.size(), fileCount);
        boolean csvFileExists = false;
        for (S3ObjectSummary s3Obj : s3Objects) {
            if (s3Obj.getKey().contains(".csv")) {
                csvFileExists = true;
                s3CsvObjectKeys.add(s3Obj.getKey());
            }
        }
        assertTrue(csvFileExists, "CSV file doesnot exists");
        return s3CsvObjectKeys;
    }

    private void assertS3CsvContents(List<String> s3ObjKeys, List<String> expectedColHeaders,
                                     Integer addRowCount, Integer removeRowCount) {
        for (String s3ObjKey : s3ObjKeys) {
            String fileName = s3ObjKey.substring(s3ObjKey.lastIndexOf('/'));
            if (fileName.contains("add")) {
                assertS3CSVContents(s3ObjKey, expectedColHeaders, addRowCount);
            } else if (fileName.contains("delete")) {
                assertS3CSVContents(s3ObjKey, expectedColHeaders, removeRowCount);
            } else {
                log.info("CSV is neither add or delete: " + s3ObjKey);
            }
        }
    }

    private void assertS3CSVContents(String s3ObjKey, List<String> expectedColHeaders, int expectedRowsIncludingHeader) {
        InputStream inputStream = s3Service.readObjectAsStream(exportS3Bucket, s3ObjKey);
        try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
            List<String[]> csvRows = reader.readAll();
            log.info(String.format("There are %d rows in file %s.", csvRows.size(), s3ObjKey));
            for (String[] row : csvRows) {
                log.info(Arrays.deepToString(row));
            }
            assertTrue(listEqualsIgnoreOrder(expectedColHeaders, Arrays.asList(csvRows.get(0))));
            assertEquals(csvRows.size(), expectedRowsIncludingHeader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
        return new HashSet<>(list1).equals(new HashSet<>(list2));
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
}
