package com.latticeengines.cdl.workflow.steps.campaign;

import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLiveRampLaunchArtifactStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class GenerateLiveRampLaunchArtifactsTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateLiveRampLaunchArtifactsTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Inject
    @InjectMocks
    private GenerateLiveRampLaunchArtifacts generateLiveRampLaunchArtifacts;

    @Mock
    private PlayProxy playProxy;

    @Mock
    private MetadataProxy metadataProxy;

    @Spy
    private BulkMatchService bulkMatchService;

    private GenerateLiveRampLaunchArtifactStepConfiguration configuration;
    private ExecutionContext executionContext;
    private CustomerSpace customerSpace;

    private String addContactsAvroLocation;
    private String removeContactsAvroLocation;

    private static final String[] TEST_JOB_LEVELS = { "Software Engineer", "CEO", "President" };
    private static final String[] TEST_JOB_FUNCTIONS = { "Marketing", "Research", "HR" };
    private static final String INPUT_ADD_ACCOUNTS_TABLE = "INPUT_ADD_ACCOUNTS_TABLE";
    private static final String INPUT_REMOVE_ACCOUNTS_TABLE = "INPUT_REMOVE_ACCOUNTS_TABLE";
    private static final String MATCHED_ADD_ACCOUNTS_LOCATION = "MATCHED_ADD_ACCOUNTS_LOCATION";
    private static final String MATCHED_REMOVE_ACCOUNTS_LOCATION = "MATCHED_REMOVE_ACCOUNTS_LOCATION";
    private static final Integer MATCHED_ADD_ACCOUNTS_ROWS = 123;
    private static final Integer MATCHED_REMOVED_ACCOUNTS_ROWS = 321;
    private static final String FAKE_EXECUTION_ID = "1234321";

    @BeforeClass(groups = "functional")
    public void setupTest() throws Exception {
        customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);
        setupHdfs();

        MediaMathChannelConfig testChannelConfig = new MediaMathChannelConfig();
        testChannelConfig.setJobFunctions(TEST_JOB_FUNCTIONS);
        testChannelConfig.setJobLevels(TEST_JOB_LEVELS);

        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setChannelConfig(testChannelConfig);

        configuration = new GenerateLiveRampLaunchArtifactStepConfiguration();
        configuration.setCustomerSpace(customerSpace);
        configuration.setExecutionId(FAKE_EXECUTION_ID);
        generateLiveRampLaunchArtifacts.setConfiguration(configuration);

        MockitoAnnotations.initMocks(this);
        
        Table fakeTable = Mockito.mock(Table.class);

        HdfsDataUnit addDataUnit = new HdfsDataUnit();
        addDataUnit.setPath(MATCHED_ADD_ACCOUNTS_LOCATION);
        HdfsDataUnit removeDataUnit = new HdfsDataUnit();
        removeDataUnit.setPath(MATCHED_REMOVE_ACCOUNTS_LOCATION);

        Mockito.doReturn(addDataUnit, removeDataUnit).when(fakeTable).toHdfsDataUnit(any());

        Mockito.doReturn(channel).when(playProxy).getChannelById(any(), any(), any());
        Mockito.doNothing().when(metadataProxy).createTable(any(), any(), any());
        Mockito.doReturn(fakeTable).when(metadataProxy).getTable(any(), any());

        MatchCommand fakeAddMatch = new MatchCommand();
        fakeAddMatch.setResultLocation(addContactsAvroLocation);
        fakeAddMatch.setRowsMatched(MATCHED_ADD_ACCOUNTS_ROWS);

        MatchCommand fakeRemoveMatch = new MatchCommand();
        fakeRemoveMatch.setResultLocation(removeContactsAvroLocation);
        fakeRemoveMatch.setRowsMatched(MATCHED_REMOVED_ACCOUNTS_ROWS);

        Mockito.doReturn(fakeAddMatch, fakeRemoveMatch).when(bulkMatchService).match(any(), any());

    }

    @Test(groups = "functional")
    public void testBulkMathTps() {
        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

        generateLiveRampLaunchArtifacts.putObjectInContext(
                GenerateLiveRampLaunchArtifacts.ADDED_ACCOUNTS_DELTA_TABLE,
                INPUT_ADD_ACCOUNTS_TABLE);
        generateLiveRampLaunchArtifacts.putObjectInContext(
                GenerateLiveRampLaunchArtifacts.REMOVED_ACCOUNTS_DELTA_TABLE,
                INPUT_REMOVE_ACCOUNTS_TABLE);

        generateLiveRampLaunchArtifacts.execute();

        String addedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE, String.class);
        String removedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE, String.class);

        Map<String, Long> counts = generateLiveRampLaunchArtifacts
                .getMapObjectFromContext(GenerateLiveRampLaunchArtifacts.DELTA_TABLE_COUNTS, String.class, Long.class);

        Assert.assertNotNull(addedContactsDeltaTable);
        Assert.assertNotNull(removedContactsDeltaTable);
        log.info(String.format("Table %s was created", addedContactsDeltaTable));
        log.info(String.format("Table %s was created", removedContactsDeltaTable));

        log.info(counts.toString());
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE),
                Long.valueOf(MATCHED_ADD_ACCOUNTS_ROWS));
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE),
                Long.valueOf(MATCHED_REMOVED_ACCOUNTS_ROWS));
    }

    @AfterClass(groups = "functional")
    private void cleanupHdfs() {
        try {
            String contractPath = PathBuilder.buildContractPath(podId, customerSpace.getContractId()).toString();
            log.info("Removing dir: " + contractPath);
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
            log.info("Removing temp Liveramp dirs");
            HdfsUtils.rmdir(yarnConfiguration, "/tmp/addLiveRampResult/");
            HdfsUtils.rmdir(yarnConfiguration, "/tmp/removeLiveRampResult/");
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private void setupHdfs() throws IOException, URISyntaxException {
        String tableAvroPath = PathBuilder.buildDataTablePath(podId, customerSpace).toString();
        createDirsIfDoesntExist(tableAvroPath);
        moveAvroFilesToHDFS();
    }

    private void moveAvroFilesToHDFS() throws IOException, URISyntaxException {
        createDirsIfDoesntExist("/tmp/addLiveRampResult/");
        createDirsIfDoesntExist("/tmp/removeLiveRampResult/");

        URL url = ClassLoader.getSystemResource("com/latticeengines/cdl/workflow/campaign/addLiverampBlock.avro");
        File localFile = new File(url.getFile());
        log.info("Taking file from: " + localFile.getAbsolutePath());
        addContactsAvroLocation = "/tmp/addLiveRampResult/addLiverampBlock.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), addContactsAvroLocation);

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, addContactsAvroLocation));
        log.info("Added Match Block uploaded to: " + addContactsAvroLocation);
       
        url = ClassLoader.getSystemResource("com/latticeengines/cdl/workflow/campaign/removeLiverampBlock.avro");
        localFile = new File(url.getFile());
        log.info("Taking file from: " + localFile.getAbsolutePath());
        removeContactsAvroLocation = "/tmp/removeLiveRampResult/removeLiverampBlock.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), removeContactsAvroLocation);

        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, removeContactsAvroLocation));
        log.info("Removed Match Block uploaded to: " + removeContactsAvroLocation);
    }

    private void createDirsIfDoesntExist(String dir) throws IOException {
        if (!HdfsUtils.isDirectory(yarnConfiguration, dir)) {
            log.info("Dir does not exist, creating dir: " + dir);
            HdfsUtils.mkdir(yarnConfiguration, dir);
        }
    }
}
