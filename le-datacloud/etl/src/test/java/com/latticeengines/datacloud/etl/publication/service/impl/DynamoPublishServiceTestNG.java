package com.latticeengines.datacloud.etl.publication.service.impl;

import static com.latticeengines.datacloud.etl.publication.service.impl.DynamoPublishService.TAG_LE_ENV;
import static com.latticeengines.datacloud.etl.publication.service.impl.DynamoPublishService.TAG_LE_PRODUCT;
import static com.latticeengines.datacloud.etl.publication.service.impl.DynamoPublishService.TAG_LE_PRODUCT_VALUE;
import static org.mockito.ArgumentMatchers.any;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.publication.DynamoDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.yarn.exposed.service.JobService;

public class DynamoPublishServiceTestNG extends DataCloudEtlFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DynamoPublishServiceTestNG.class);

    private static final String PUBLICATION_NAME = "TestAccountLookupPublication";
    private static final String CURRENT_VERSION = HdfsPathBuilder.dateFormat.format(new Date());
    private static final String SUBMITTER = DataCloudConstants.SERVICE_TENANT;

    private static final String SOURCE_NAME = "AccountMaster";
    private static final String RECORD_TYPE = "LatticeAccount";
    private static final String VERSION = "1.0.0";

    @Resource(name = "dynamoPublishService")
    private PublishService<PublishToDynamoConfiguration> publishService;

    @Inject
    private PublicationEntityMgr publicationEntityMgr;

    @Inject
    private PublicationProgressEntityMgr progressEntityMgr;

    private Publication publication;

    private final Map<String, MockDynamoTable> dynamoTables = new ConcurrentHashMap<>();

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(this.getClass().getSimpleName());
        Assert.assertEquals(podId, this.getClass().getSimpleName());

        uploadBaseSourceFile(SOURCE_NAME, "AMBucketTest_AM", CURRENT_VERSION);
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(SOURCE_NAME), CURRENT_VERSION);

        publicationEntityMgr.removePublication(PUBLICATION_NAME);
        publication = createPublication();
        DynamoService dynamoService = new TestDynamoSerivceImpl();
        ((DynamoPublishService) publishService).setOverridingDynamoService(dynamoService);
        ((DynamoPublishService) publishService).setEaiProxy(new TestEaiProxy(dynamoTables));
        ((DynamoPublishService) publishService).setJobService(mockJobService());
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        publicationEntityMgr.removePublication(PUBLICATION_NAME);
    }

    @Test(groups = "functional")
    public void test() {
        PublicationProgress progress = progressEntityMgr.startNewProgress(publication, createDestination(),
                CURRENT_VERSION, SUBMITTER);

        publishService.publish(progress, createConfiguration());

        String tableName = DynamoPublishService.convertToFabricStoreName(RECORD_TYPE + VERSION);
        Assert.assertEquals(dynamoTables.size(), 1, "Should have 1 table");
        Assert.assertTrue(dynamoTables.containsKey(tableName), "Should have table " + tableName);
        Assert.assertEquals(dynamoTables.get(tableName).size, 100);
        verifyTags(dynamoTables.get(tableName));
        verifyThroughput(dynamoTables.get(tableName));

        progress = progressEntityMgr.runNewProgress(publication, createDestination(), CURRENT_VERSION, SUBMITTER);
        publishService.publish(progress, createConfiguration());
        Assert.assertEquals(dynamoTables.size(), 1, "Should have 1 table");
        Assert.assertTrue(dynamoTables.containsKey(tableName), "Should have table " + tableName);
        Assert.assertEquals(dynamoTables.get(tableName).size, 100);
        verifyTags(dynamoTables.get(tableName));
        verifyThroughput(dynamoTables.get(tableName));

        PublishToDynamoConfiguration configuration = createConfiguration();
        configuration.setPublicationStrategy(PublicationConfiguration.PublicationStrategy.APPEND);
        progress = progressEntityMgr.runNewProgress(publication, createDestination(), CURRENT_VERSION, SUBMITTER);
        publishService.publish(progress, configuration);
        Assert.assertEquals(dynamoTables.size(), 1, "Should have 1 table");
        Assert.assertTrue(dynamoTables.containsKey(tableName), "Should have table " + tableName);
        Assert.assertEquals(dynamoTables.get(tableName).size, 200);
        verifyTags(dynamoTables.get(tableName));
        verifyThroughput(dynamoTables.get(tableName));
    }

    private DynamoDestination createDestination() {
        DynamoDestination destination = new DynamoDestination();
        destination.setVersion(VERSION);
        return destination;
    }

    private Publication createPublication() {
        Publication publication = new Publication();
        publication.setPublicationName(PUBLICATION_NAME);
        publication.setSourceName(SOURCE_NAME);
        publication.setNewJobMaxRetry(1);
        publication.setPublicationType(Publication.PublicationType.DYNAMO);
        publication.setMaterialType(Publication.MaterialType.SOURCE);
        publication.setDestinationConfiguration(createConfiguration());
        return publicationEntityMgr.addPublication(publication);
    }

    private PublishToDynamoConfiguration createConfiguration() {
        PublishToDynamoConfiguration configuration = new PublishToDynamoConfiguration();
        configuration.setEntityClass(LatticeAccount.class.getCanonicalName());
        configuration.setRecordType(RECORD_TYPE);
        configuration.setPublicationStrategy(PublicationConfiguration.PublicationStrategy.REPLACE);
        configuration.setAlias(PublishToDynamoConfiguration.Alias.QA);

        configuration.setLoadingReadCapacity(5L);
        configuration.setLoadingWriteCapacity(100L);
        configuration.setRuntimeReadCapacity(100L);
        configuration.setRuntimeWriteCapacity(5L);

        return configuration;
    }

    private void verifyTags(MockDynamoTable table) {
        Assert.assertTrue(table.tags.containsKey(TAG_LE_ENV));
        Assert.assertTrue(table.tags.containsKey(TAG_LE_PRODUCT));
        Assert.assertEquals(table.tags.get(TAG_LE_ENV), "qa");
        Assert.assertEquals(table.tags.get(TAG_LE_PRODUCT), TAG_LE_PRODUCT_VALUE);
    }

    private void verifyThroughput(MockDynamoTable table) {
        Assert.assertEquals(table.readCapacityUnits, 100L);
        Assert.assertEquals(table.writeCapacityUnits, 5L);
    }

    private JobService mockJobService() {
        JobService jobService = Mockito.mock(JobService.class);
        JobStatus completedJobStats = new JobStatus();
        completedJobStats.setStatus(FinalApplicationStatus.SUCCEEDED);
        Mockito.when(jobService.waitFinalJobStatus(any(String.class), any(Integer.class))).thenReturn(completedJobStats);
        return jobService;
    }


    private static class MockDynamoTable {
        private static final Logger log = LoggerFactory.getLogger(MockDynamoTable.class);

        String tableName;
        long readCapacityUnits;
        long writeCapacityUnits;
        long size = 0L;
        Map<String, String> tags = new HashMap<>();

        MockDynamoTable(String tableName, long readCapacityUnits, long writeCapacityUnits) {
            this.tableName = tableName;
            log.info(String.format("Create dynamo table %s", tableName));
            updateThroughput(readCapacityUnits, writeCapacityUnits);
        }

        void updateThroughput(long readCapacityUnits, long writeCapacityUnits) {
            this.readCapacityUnits = readCapacityUnits;
            this.writeCapacityUnits = writeCapacityUnits;
            log.info(String.format("Update throughput of %s: read = %d, write = %d", tableName, readCapacityUnits,
                    writeCapacityUnits));
        }

        void upload() {
            size += 100;
            log.info("Uploaded 100 records to " + tableName);
        }

        void tag(Map<String, String> tags) {
            this.tags.putAll(tags);
        }

    }

    private class TestDynamoSerivceImpl implements DynamoService {

        @Override
        public AmazonDynamoDB getClient() {
            return null;
        }

        @Override
        public AmazonDynamoDB getRemoteClient() {
            return null;
        }

        @Override
        public DynamoDB getDynamo() { return null; }

        @Override
        public void switchToLocal(boolean local) {
            if (local) {
                log.info("Switch dynamo service to local mode.");
            } else {
                log.info("Switch dynamo service to remote mode.");
            }
        }

        @Override
        public Table createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName,
                                 String partitionKeyType, String sortKeyName, String sortKeyType) {
            MockDynamoTable dynamoTable = new MockDynamoTable(tableName, readCapacityUnits, writeCapacityUnits);
            dynamoTables.put(tableName, dynamoTable);
            return null;
        }

        @Override
        public void deleteTable(String tableName) {
            dynamoTables.remove(tableName);
        }

        @Override
        public boolean hasTable(String tableName) {
            return dynamoTables.containsKey(tableName);
        }

        @Override
        public void updateTableThroughput(String tableName, long readCapacity, long writeCapacity) {
            MockDynamoTable table = dynamoTables.get(tableName);
            table.updateThroughput(readCapacity, writeCapacity);
        }

        @Override
        public void tagTable(String tableName, Map<String, String> tags) {
            MockDynamoTable table = dynamoTables.get(tableName);
            table.tag(tags);
        }

        @Override
        public TableDescription describeTable(String tableName) {
            return null;
        }
    }

    private static class TestEaiProxy extends EaiProxy {
        private static final Logger log = LoggerFactory.getLogger(TestEaiProxy.class);

        private final Map<String, MockDynamoTable> dynamoTables;

        TestEaiProxy(Map<String, MockDynamoTable> dynamoTables) {
            this.dynamoTables = dynamoTables;
        }

        @Override
        public AppSubmission submitEaiJob(EaiJobConfiguration eaiJobConfig) {
            log.info(String.format("Received EAI configuration:\n%s", JsonUtils.pprint(eaiJobConfig)));
            String repository = eaiJobConfig.getProperty("eai.export.dynamo.repository");
            String recordType = eaiJobConfig.getProperty("eai.export.dynamo.record.type");
            String tableName = String.format("_REPO_%s_RECORD_%s", repository, recordType);
            dynamoTables.get(tableName).upload();
            ApplicationId appId = ConverterUtils.toApplicationId("application_1501641968014_10281");
            return new AppSubmission(Collections.singletonList(appId));
        }
    }

}
