package com.latticeengines.datafabric.entitymanager.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricMessageManager;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricNode;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatus;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatusEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class GenericFabricMessageManagerImplFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenericFabricMessageManagerImplFunctionalTestNG.class);

    private static final String FILE_PATTERN = "Snapshot/*/*.avro";

    @Resource(name = "genericFabricMessageManager")
    private GenericFabricMessageManager<SampleEntity> entityManager;

    @Autowired
    private FabricMessageService messageService;

    Set<String> batchIds = new HashSet<>();

    private Camille camille;

    @BeforeClass(groups = "functional")
    public void setUp() throws Exception {
        camille = CamilleEnvironment.getCamille();
        cleanHDFS();
    }

    private void cleanHDFS() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, BASE_DIR + "/testGenericFile1");
        HdfsUtils.rmdir(yarnConfiguration, BASE_DIR + "/testGenericFile2");
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        for (String batchId : batchIds) {
            try {
                entityManager.cleanup(batchId);
            } catch (Exception ex) {
                log.warn("Failed to cleanup, batchId=" + batchId, ex);
            }
        }
        CamilleEnvironment.stop();
        cleanHDFS();
    }

    @Test(groups = "functional", enabled = true)
    public void createBatchId() throws Exception {
        String id = entityManager.createUniqueBatchId(5L);
        batchIds.add(id);
        Assert.assertNotNull(id);

        String data = messageService.readData(id);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 5);
        Assert.assertEquals(node.getName(), id);

        Path path = CamilleEnvironment.getFabricEntityPath(id);
        Assert.assertTrue(camille.exists(path));

    }

    @Test(groups = "functional", enabled = true)
    public void createOrGetBatchName() throws Exception {
        String name = entityManager.createOrGetNamedBatchId("connectors", 6L, true);
        batchIds.add(name);
        Assert.assertNotNull(name);

        String data = messageService.readData(name);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 6);
        Assert.assertEquals(node.getName(), "connectors");

    }

    @Test(groups = "functional", enabled = true)
    public void updateBatchCountFinished() throws Exception {
        // bulk use case
        String name = entityManager.createOrGetNamedBatchId("connectors", 10L, true);
        batchIds.add(name);
        Assert.assertNotNull(name);

        String data = messageService.readData(name);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 10);
        Assert.assertEquals(node.getName(), "connectors");
        GenericFabricStatus batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);

        entityManager.updateBatchCount(name, 3, true);
        data = messageService.readData(name);
        Assert.assertNotNull(data);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 3);
        Assert.assertEquals(node.getTotalCount(), 10);
        Assert.assertEquals(node.getName(), "connectors");
        batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.30f));

        entityManager.updateBatchCount(name, 6, true);
        data = messageService.readData(name);
        Assert.assertNotNull(data);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 9);
        Assert.assertEquals(node.getTotalCount(), 10);
        Assert.assertEquals(node.getName(), "connectors");
        batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.9f));

        entityManager.updateBatchCount(name, 1, true);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.FINISHED);
        Assert.assertEquals(batchStatus.getProgress(), new Float(1.0f));

        // streaming continuously use case
        name = entityManager.createOrGetNamedBatchId("connectors", null, true);
        batchIds.add(name);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getTotalCount(), -1);
        entityManager.updateBatchCount(name, 8, true);
        entityManager.updateBatchCount(name, 10, true);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 18);
        batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.01f));

        name = entityManager.createUniqueBatchId(null);
        batchIds.add(name);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getTotalCount(), -1);
        entityManager.updateBatchCount(name, 8, true);
        entityManager.updateBatchCount(name, 10, true);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 18);
        batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.01f));
    }

    @Test(groups = "functional", enabled = true)
    public void updateBatchCountNotFinished() throws Exception {
        // bulk use case
        String name = entityManager.createOrGetNamedBatchId("connectors", 10L, true);
        batchIds.add(name);
        Assert.assertNotNull(name);

        String data = messageService.readData(name);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 10);
        Assert.assertEquals(node.getName(), "connectors");
        GenericFabricStatus batchStatus = entityManager.getBatchStatus(name);
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);

        entityManager.updateBatchCount(name, 3, true);
        entityManager.updateBatchCount(name, 3, false);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 3);
        Assert.assertEquals(node.getFailedCount(), 3);
        Assert.assertEquals(node.getTotalCount(), 10);
        Assert.assertEquals(node.getName(), "connectors");
        batchStatus = entityManager.getBatchStatus(name);
        log.info(batchStatus.getMessage());
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.3f));
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.ERROR);

        name = entityManager.createOrGetNamedBatchId("connectors", null, true);
        batchIds.add(name);
        Assert.assertNotNull(name);

        entityManager.updateBatchCount(name, 3, false);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getFinishedCount(), 0);
        Assert.assertEquals(node.getFailedCount(), 3);
        Assert.assertEquals(node.getTotalCount(), -1);
        Assert.assertEquals(node.getName(), "connectors");
        batchStatus = entityManager.getBatchStatus(name);
        log.info(batchStatus.getMessage());
        Assert.assertEquals(batchStatus.getProgress(), new Float(0.01f));
        Assert.assertEquals(batchStatus.getStatus(), GenericFabricStatusEnum.PROCESSING);

    }

    @Test(groups = "manual", enabled = true)
    public void publishEntity() throws Exception {

        long RECORD_COUNT = 1;
        cleanHDFS();

        String batchId = entityManager.createUniqueBatchId(RECORD_COUNT);
        batchIds.add(batchId);

        GenericRecordRequest recordRequest = new GenericRecordRequest();
        recordRequest.setBatchId(batchId).setCustomerSpace("generic").setStores(Arrays.asList(FabricStoreEnum.HDFS))
                .setRepositories(Arrays.asList("testGenericFile1")).setId(1 + "");

        SampleEntity entity = new SampleEntity();
        entity.setId("" + 1);
        entity.setCompany("company" + 1);
        entity.setName("lattice" + 1);
        entity.setLatticeId("latticeId" + 1);

        entityManager.publishEntity(recordRequest, entity, SampleEntity.class);

        waitInSeconds(batchId, 10);

        long count1 = AvroUtils.count(yarnConfiguration, BASE_DIR + "/testGenericFile1/" + FILE_PATTERN);
        Assert.assertEquals(count1, RECORD_COUNT);

        List<GenericRecord> records1 = AvroUtils.getDataFromGlob(yarnConfiguration,
                BASE_DIR + "/testGenericFile1/" + FILE_PATTERN);
        Assert.assertEquals(records1.get(0).get("latticeId").toString(), "latticeId1");

    }

    @Test(groups = "manual", enabled = true)
    public void publishRecord() throws Exception {

        long RECORD_COUNT = 10;
        cleanHDFS();

        String batchId1 = entityManager.createUniqueBatchId(RECORD_COUNT);
        batchIds.add(batchId1);
        List<GenericRecordRequest> recordRequests = new ArrayList<>();
        List<GenericRecord> records = new ArrayList<>();
        // schema 1
        getRecords(batchId1, recordRequests, records, RECORD_COUNT, 1);
        for (int i = 0; i < records.size(); i++) {
            entityManager.publishRecord(recordRequests.get(i), records.get(i));
        }

        // schema 2
        recordRequests.clear();
        records.clear();
        String batchId2 = entityManager.createUniqueBatchId(RECORD_COUNT);
        batchIds.add(batchId2);
        getRecords(batchId2, recordRequests, records, RECORD_COUNT, 2);
        for (int i = 0; i < records.size(); i++) {
            entityManager.publishRecord(recordRequests.get(i), records.get(i));
        }

        waitInSeconds(batchId1, 20);
        waitInSeconds(batchId2, 20);

        long count1 = AvroUtils.count(yarnConfiguration, BASE_DIR + "/testGenericFile1/" + FILE_PATTERN);
        long count2 = AvroUtils.count(yarnConfiguration, BASE_DIR + "/testGenericFile2/" + FILE_PATTERN);
        Assert.assertEquals(count1, count2);
        Assert.assertEquals(count1, RECORD_COUNT);

        List<GenericRecord> records1 = AvroUtils.getDataFromGlob(yarnConfiguration,
                BASE_DIR + "/testGenericFile1/" + FILE_PATTERN);
        Assert.assertTrue(records1.get(0).get("age") == null);
        List<GenericRecord> records2 = AvroUtils.getDataFromGlob(yarnConfiguration,
                BASE_DIR + "/testGenericFile2/" + FILE_PATTERN);
        Assert.assertTrue(records2.get(0).get("age") != null);

    }

    @Test(groups = "manual", enabled = true)
    public void performance() throws Exception {

        cleanHDFS();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        long startTime = System.currentTimeMillis();
        long LARGE_RECORD_COUNT = 1_000_000L;
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            // schema 1
            addThread(pool, LARGE_RECORD_COUNT, futures, 1);

            // schema 2
            addThread(pool, LARGE_RECORD_COUNT, futures, 2);

            for (Future<Boolean> future : futures) {
                Boolean result = future.get(5, TimeUnit.HOURS);
                Assert.assertTrue(result);
            }

            long count1 = AvroUtils.count(yarnConfiguration, BASE_DIR + "/testGenericFile1/" + FILE_PATTERN);
            long count2 = AvroUtils.count(yarnConfiguration, BASE_DIR + "/testGenericFile2/" + FILE_PATTERN);
            Assert.assertEquals(count1, count2);
            Assert.assertEquals(count1, LARGE_RECORD_COUNT);

            List<GenericRecord> records1 = AvroUtils.getDataFromGlob(yarnConfiguration,
                    BASE_DIR + "/testGenericFile1/Snapshot/*/*.avro");
            Assert.assertTrue(records1.get(0).get("age") == null);
            List<GenericRecord> records2 = AvroUtils.getDataFromGlob(yarnConfiguration,
                    BASE_DIR + "/testGenericFile2/Snapshot/*/*.avro");
            Assert.assertTrue(records2.get(0).get("age") != null);

        } catch (Exception ex) {
            log.error("Failed for performance!", ex);
            Assert.fail("Failed for perfomance!" + ex.getMessage());

        } finally {
            pool.shutdown();
            log.info("Finished within "
                    + DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.SSS")
                    + " for records=" + LARGE_RECORD_COUNT);
        }
    }

    private void addThread(ExecutorService pool, long recordCount, List<Future<Boolean>> futures, int type) {
        String batchId = entityManager.createUniqueBatchId(recordCount);
        batchIds.add(batchId);
        List<GenericRecordRequest> recordRequests = new ArrayList<>();
        List<GenericRecord> records = new ArrayList<>();
        getRecords(batchId, recordRequests, records, recordCount, type);
        ConnectorCallable callable = new ConnectorCallable(batchId, recordRequests, records);
        futures.add(pool.submit(callable));
    }

    private void waitInSeconds(String batchId, int timeoutInSeconds) throws InterruptedException {
        long start = System.currentTimeMillis();
        do {
            GenericFabricStatus status = entityManager.getBatchStatus(batchId);
            if (status.getStatus().equals(GenericFabricStatusEnum.ERROR)
                    || status.getStatus().equals(GenericFabricStatusEnum.UNKNOWN)) {
                log.error("Failed for batchId=" + batchId + " Error=" + status.getMessage());
                throw new RuntimeException("Failed for batchId=" + batchId);
            }
            if (status.getStatus().equals(GenericFabricStatusEnum.FINISHED)) {
                log.info("Finished batchId=" + batchId);
                return;
            }
            log.info("BatchId=" + batchId + " Progressing=" + String.format("%.0f", status.getProgress() * 100) + "%");
            Thread.sleep(500L);
        } while ((System.currentTimeMillis() - start) < (timeoutInSeconds * 1000));

        throw new RuntimeException("Failed for timeout. batchId=" + batchId);
    }

    private List<GenericRecord> getRecords(String batchId, List<GenericRecordRequest> recordRequests,
            List<GenericRecord> records, long count, int type) {
        GenericRecordBuilder builder = null;
        if (type == 1) {
            builder = new GenericRecordBuilder(buildSchem1());
        } else {
            builder = new GenericRecordBuilder(buildSchem2());
        }

        for (int i = 0; i < count; i++) {
            builder.set("RowId", Long.valueOf(i)).set("Id", "id" + i).set("FirstLastName", "JohnSmith" + i);
            if (type != 1) {
                builder.set("age", 10 + i);
            }
            records.add(builder.build());

            GenericRecordRequest recordRequest = new GenericRecordRequest();
            if (type == 1) {
                String testFile = "testGenericFile" + type;
                List<FabricStoreEnum> stores = Arrays.asList(FabricStoreEnum.HDFS);
                List<String> repositories = Arrays.asList(testFile);
                recordRequest.setBatchId(batchId).setCustomerSpace("generic").setStores(stores)
                        .setRepositories(repositories).setId(i + "");
            }
            if (type == 2) {
                List<FabricStoreEnum> stores = Arrays.asList(FabricStoreEnum.HDFS, FabricStoreEnum.S3);
                List<String> repositories = Arrays.asList("testGenericFile" + type, "testGenericFile" + type);
                recordRequest.setBatchId(batchId).setCustomerSpace("generic").setStores(stores)
                        .setRepositories(repositories).setId(i + "").setRecordType("S3Connector");
            }
            recordRequests.add(recordRequest);
        }

        return records;
    }

    private Schema buildSchem1() {
        String schemaString = "{\"namespace\": \"FabricGenericRecord\", \"type\": \"record\", "
                + "\"name\": \"FabricGenericRecord1\"," + "\"fields\": [" + "{\"name\": \"RowId\", \"type\": \"long\"},"
                + "{\"name\": \"Id\", \"type\": \"string\"}," + "{\"name\": \"FirstLastName\", \"type\": \"string\"}"
                + "]}";

        log.info(schemaString);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private Schema buildSchem2() {
        String schemaString = "{\"namespace\": \"FabricGenericRecord\", \"type\": \"record\", "
                + "\"name\": \"FabricGenericRecord2\"," + "\"fields\": [" + "{\"name\": \"RowId\", \"type\": \"long\"},"
                + "{\"name\": \"Id\", \"type\": \"string\"}," + "{\"name\": \"FirstLastName\", \"type\": \"string\"},"
                + "{\"name\": \"age\", \"type\": \"int\"}" + "]}";

        log.info(schemaString);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private class ConnectorCallable implements Callable<Boolean> {
        private List<GenericRecordRequest> recordRequests;
        private List<GenericRecord> records;
        private String batchId;

        public ConnectorCallable(String batchId, List<GenericRecordRequest> recordRequests,
                List<GenericRecord> records) {
            this.batchId = batchId;
            this.recordRequests = recordRequests;
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                for (int i = 0; i < records.size(); i++) {
                    entityManager.publishRecord(recordRequests.get(i), records.get(i));
                    if (i % 1000 == 0) {
                        log.info("Publishing at record=" + i + " for batchId=" + batchId);
                    }
                }
                waitInSeconds(batchId, 3_600);
                return Boolean.TRUE;
            } catch (Exception ex) {
                log.error("Failed to publish!", ex);
                return Boolean.FALSE;
            }
        }

    }
}
