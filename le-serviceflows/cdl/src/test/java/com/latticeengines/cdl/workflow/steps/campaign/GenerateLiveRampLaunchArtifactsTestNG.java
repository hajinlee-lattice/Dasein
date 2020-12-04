package com.latticeengines.cdl.workflow.steps.campaign;

import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
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
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class GenerateLiveRampLaunchArtifactsTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateLiveRampLaunchArtifactsTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Spy
    @InjectMocks
    private GenerateLiveRampLaunchArtifacts generateLiveRampLaunchArtifacts;

    @Mock
    private PlayProxy playProxy;

    @Mock
    private MetadataProxy metadataProxy;

    @Spy
    private BulkMatchService bulkMatchService;

    @Mock
    private SparkJobService sparkJobService;

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
    private static Long ZERO_EXPECTED = 0L;

    @BeforeClass(groups = "functional")
    public void setupTest() throws Exception {
        MockitoAnnotations.initMocks(this);

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

        Mockito.doReturn(channel).when(playProxy).getChannelById(any(), any(), any());
        Mockito.doNothing().when(metadataProxy).createTable(any(), any(), any());

        Mockito.doAnswer(new Answer<SparkJobResult>() {
            @Override
            public SparkJobResult answer(InvocationOnMock invocation) throws Throwable {
                HdfsDataUnit addContact = (HdfsDataUnit) invocation.getArguments()[0];
                HdfsDataUnit removeContact = (HdfsDataUnit) invocation.getArguments()[1];
                SparkJobResult res = new SparkJobResult();
                res.setTargets(Arrays.asList(new HdfsDataUnit[] { addContact, removeContact }));
                return res;
            }
        }).when(generateLiveRampLaunchArtifacts).executeSparkJob(any(), any());

        // Have to reflect fields into baseSparkStep b/c @Inject does not work
        // with @Spy and @InjectMocks combined
        FieldUtils.writeField(generateLiveRampLaunchArtifacts, "podId", podId, true);
        FieldUtils.writeField(generateLiveRampLaunchArtifacts, "yarnConfiguration", yarnConfiguration, true);
    }

    @Test(groups = "functional")
    public void testBulkMathTpsForAddAndDelete() {
        setupMockitoForAddAndDelete();

        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

        generateLiveRampLaunchArtifacts.putObjectInContext(GenerateLiveRampLaunchArtifacts.ADDED_ACCOUNTS_DELTA_TABLE,
                INPUT_ADD_ACCOUNTS_TABLE);
        generateLiveRampLaunchArtifacts.putObjectInContext(GenerateLiveRampLaunchArtifacts.REMOVED_ACCOUNTS_DELTA_TABLE,
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

    private void setupMockitoForAddAndDelete() {
        Table fakeTable = Mockito.mock(Table.class);

        HdfsDataUnit addDataUnit = new HdfsDataUnit();
        addDataUnit.setPath(MATCHED_ADD_ACCOUNTS_LOCATION);
        HdfsDataUnit removeDataUnit = new HdfsDataUnit();
        removeDataUnit.setPath(MATCHED_REMOVE_ACCOUNTS_LOCATION);

        Mockito.doReturn(addDataUnit, removeDataUnit).when(fakeTable).toHdfsDataUnit(any());

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
    public void testBulkMathTpsForOnlyAdd() {
        setupMockitoForOnlyAdd();

        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

        generateLiveRampLaunchArtifacts.putObjectInContext(GenerateLiveRampLaunchArtifacts.ADDED_ACCOUNTS_DELTA_TABLE,
                INPUT_ADD_ACCOUNTS_TABLE);

        generateLiveRampLaunchArtifacts.execute();

        String addedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE, String.class);
        String removedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE, String.class);

        Map<String, Long> counts = generateLiveRampLaunchArtifacts
                .getMapObjectFromContext(GenerateLiveRampLaunchArtifacts.DELTA_TABLE_COUNTS, String.class, Long.class);

        Assert.assertNotNull(addedContactsDeltaTable);
        Assert.assertNull(removedContactsDeltaTable);
        log.info(String.format("Table %s was created", addedContactsDeltaTable));
        log.info(String.format("Table %s was not created", removedContactsDeltaTable));

        log.info(counts.toString());
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE),
                Long.valueOf(MATCHED_ADD_ACCOUNTS_ROWS));
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE),
                ZERO_EXPECTED);
    }

    private void setupMockitoForOnlyAdd() {
        Table fakeTable = Mockito.mock(Table.class);

        HdfsDataUnit addDataUnit = new HdfsDataUnit();
        addDataUnit.setPath(MATCHED_ADD_ACCOUNTS_LOCATION);

        Mockito.doReturn(addDataUnit).when(fakeTable).toHdfsDataUnit(any());

        Mockito.doReturn(fakeTable).when(metadataProxy).getTable(any(), any());

        MatchCommand fakeAddMatch = new MatchCommand();
        fakeAddMatch.setResultLocation(addContactsAvroLocation);
        fakeAddMatch.setRowsMatched(MATCHED_ADD_ACCOUNTS_ROWS);

        Mockito.doReturn(fakeAddMatch).when(bulkMatchService).match(any(), any());
    }

    @Test(groups = "functional")
    public void testBulkMathTpsForOnlyRemove() {
        setupMockitoForOnlyRemove();

        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

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

        Assert.assertNull(addedContactsDeltaTable);
        Assert.assertNotNull(removedContactsDeltaTable);
        log.info(String.format("Table %s was not created", addedContactsDeltaTable));
        log.info(String.format("Table %s was created", removedContactsDeltaTable));

        log.info(counts.toString());
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE),
                ZERO_EXPECTED);
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE),
                Long.valueOf(MATCHED_REMOVED_ACCOUNTS_ROWS));
    }

    private void setupMockitoForOnlyRemove() {
        Table fakeTable = Mockito.mock(Table.class);

        HdfsDataUnit removeDataUnit = new HdfsDataUnit();
        removeDataUnit.setPath(MATCHED_REMOVE_ACCOUNTS_LOCATION);

        Mockito.doReturn(removeDataUnit).when(fakeTable).toHdfsDataUnit(any());

        Mockito.doReturn(fakeTable).when(metadataProxy).getTable(any(), any());

        MatchCommand fakeAddMatch = new MatchCommand();
        fakeAddMatch.setResultLocation(removeContactsAvroLocation);
        fakeAddMatch.setRowsMatched(MATCHED_REMOVED_ACCOUNTS_ROWS);

        Mockito.doReturn(fakeAddMatch).when(bulkMatchService).match(any(), any());
    }

    @Test(groups = "functional")
    public void testBulkMathTpsForNoAddOrRemove() {
        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

        generateLiveRampLaunchArtifacts.execute();

        String addedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE, String.class);
        String removedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getObjectFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE, String.class);

        Map<String, Long> counts = generateLiveRampLaunchArtifacts
                .getMapObjectFromContext(GenerateLiveRampLaunchArtifacts.DELTA_TABLE_COUNTS, String.class, Long.class);

        Assert.assertNull(addedContactsDeltaTable);
        Assert.assertNull(removedContactsDeltaTable);
        log.info(String.format("Table %s was not created", addedContactsDeltaTable));
        log.info(String.format("Table %s was not created", removedContactsDeltaTable));

        log.info(counts.toString());
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE), ZERO_EXPECTED);
        Assert.assertEquals(counts.get(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE),
                ZERO_EXPECTED);
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

    private void setupHdfs() throws IOException {
        String tableAvroPath = PathBuilder.buildDataTablePath(podId, customerSpace).toString();
        createDirsIfDoesntExist(tableAvroPath);
        moveAvroFilesToHDFS();
    }

    private void moveAvroFilesToHDFS() throws IOException {
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
