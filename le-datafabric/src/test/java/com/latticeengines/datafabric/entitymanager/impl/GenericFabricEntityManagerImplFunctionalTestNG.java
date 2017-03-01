package com.latticeengines.datafabric.entitymanager.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.impl.FabricMessageServiceImpl;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricNode;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricRecord;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatus;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class GenericFabricEntityManagerImplFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    private static final long RECORD_COUNT = 10;

    private static final String POD = "FabricConnectors";
    private static final String STACK = "b";

    @Autowired
    private GenericFabricEntityManager entityManager;

    @Value("${datafabric.message.zkConnect}")
    private String zkConnect;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private Configuration conf;

    Set<String> batchIds = new HashSet<>();

    private Camille camille;

    @BeforeClass(groups = "functional")
    public void setUp() throws Exception {
        FabricMessageServiceImpl messageServiceImpl = (FabricMessageServiceImpl) messageService;
        messageServiceImpl.setPod(POD);
        messageServiceImpl.setStack(STACK);
        messageServiceImpl.setupCamille(Mode.BOOTSTRAP, POD, STACK, zkConnect);
        camille = messageServiceImpl.getCamille();

        HdfsUtils.rmdir(conf, "/tmp/testGenericFile1");
        HdfsUtils.rmdir(conf, "/tmp/testGenericFile2");
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
        HdfsUtils.rmdir(conf, "/tmp/testGenericFile1");
        HdfsUtils.rmdir(conf, "/tmp/testGenericFile2");
    }

    @Test(groups = "functional", enabled = true)
    public void createBatchId() throws Exception {
        String id = entityManager.createUniqueBatchId(5L);
        batchIds.add(id);
        Assert.assertNotNull(id);

        String data = messageService.readData(id);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 5);
        Assert.assertEquals(node.getName(), id);

        Path path = PathBuilder.buildPodDivisionPath(POD, STACK).append(id);
        Assert.assertTrue(camille.exists(path));

    }

    @Test(groups = "functional", enabled = true)
    public void createOrGetBatchName() throws Exception {
        String name = entityManager.createOrGetNamedBatchId("connectors", 6L, true);
        Assert.assertNotNull(name);
        batchIds.add(name);

        String data = messageService.readData(name);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 6);
        Assert.assertEquals(node.getName(), "connectors");

    }

    @Test(groups = "functional", enabled = true)
    public void updateBatchCount() throws Exception {
        // bulk use case
        String name = entityManager.createOrGetNamedBatchId("connectors", 7L, true);
        Assert.assertNotNull(name);
        batchIds.add(name);

        String data = messageService.readData(name);
        Assert.assertNotNull(data);
        GenericFabricNode node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 0);
        Assert.assertEquals(node.getTotalCount(), 7);
        Assert.assertEquals(node.getName(), "connectors");
        Assert.assertEquals(entityManager.getBatchStatus(name), GenericFabricStatus.PROCESSING);

        entityManager.updateBatchCount(name, 3);
        data = messageService.readData(name);
        Assert.assertNotNull(data);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 3);
        Assert.assertEquals(node.getTotalCount(), 7);
        Assert.assertEquals(node.getName(), "connectors");
        Assert.assertEquals(entityManager.getBatchStatus(name), GenericFabricStatus.PROCESSING);

        entityManager.updateBatchCount(name, 4);
        data = messageService.readData(name);
        Assert.assertNotNull(data);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 7);
        Assert.assertEquals(node.getTotalCount(), 7);
        Assert.assertEquals(node.getName(), "connectors");
        Assert.assertEquals(entityManager.getBatchStatus(name), GenericFabricStatus.FINISHED);

        // streaming continuously use case
        name = entityManager.createOrGetNamedBatchId("connectors", null, true);
        batchIds.add(name);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getTotalCount(), -1);
        entityManager.updateBatchCount(name, 8);
        entityManager.updateBatchCount(name, 10);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 18);
        Assert.assertEquals(entityManager.getBatchStatus(name), GenericFabricStatus.PROCESSING);

        name = entityManager.createUniqueBatchId(null);
        batchIds.add(name);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getTotalCount(), -1);
        entityManager.updateBatchCount(name, 8);
        entityManager.updateBatchCount(name, 10);
        data = messageService.readData(name);
        node = JsonUtils.deserialize(data, GenericFabricNode.class);
        Assert.assertEquals(node.getCount(), 18);
        Assert.assertEquals(entityManager.getBatchStatus(name), GenericFabricStatus.PROCESSING);
    }

    @Test(groups = "manual", enabled = true)
    public void publish() throws Exception {

        String batchId1 = entityManager.createUniqueBatchId(RECORD_COUNT);
        List<GenericRecordRequest> recordRequests = new ArrayList<>();
        List<GenericFabricRecord> records = new ArrayList<>();
        // schema 1
        getRecords(batchId1, recordRequests, records, RECORD_COUNT, 1);
        for (int i = 0; i < records.size(); i++) {
            entityManager.publish(recordRequests.get(i), records.get(i));
        }

        // schema 2
        recordRequests.clear();
        records.clear();
        String batchId2 = entityManager.createUniqueBatchId(RECORD_COUNT);
        getRecords(batchId2, recordRequests, records, RECORD_COUNT, 2);
        for (int i = 0; i < records.size(); i++) {
            entityManager.publish(recordRequests.get(i), records.get(i));
        }
        batchIds.add(batchId1);
        batchIds.add(batchId2);

        waitFor(batchId1, 10);
        waitFor(batchId2, 10);

        long count1 = AvroUtils.count(conf, "/tmp/testGenericFile1/*.avro");
        long count2 = AvroUtils.count(conf, "/tmp/testGenericFile2/*.avro");
        Assert.assertEquals(count1, count2);
        Assert.assertEquals(count1, RECORD_COUNT);

        List<GenericRecord> records1 = AvroUtils.getDataFromGlob(conf, "/tmp/testGenericFile1/*.avro");
        Assert.assertTrue(records1.get(0).get("age") == null);
        List<GenericRecord> records2 = AvroUtils.getDataFromGlob(conf, "/tmp/testGenericFile2/*.avro");
        Assert.assertTrue(records2.get(0).get("age") != null);

    }

    private void waitFor(String batchId, int timeoutInSeconds) throws InterruptedException {
        long start = System.currentTimeMillis();
        do {
            GenericFabricStatus status = entityManager.getBatchStatus(batchId);
            if (status.equals(GenericFabricStatus.ERROR) || status.equals(GenericFabricStatus.UNKNOWN)) {
                throw new RuntimeException("Failed for batchId=" + batchId);
            }
            if (status.equals(GenericFabricStatus.FINISHED)) {
                System.out.println("Finished batchId=" + batchId);
                return;
            }
            Thread.sleep(500L);
        } while ((System.currentTimeMillis() - start) < (timeoutInSeconds * 1000));

        throw new RuntimeException("Failed for timeout. batchId=" + batchId);
    }

    private List<GenericFabricRecord> getRecords(String id, List<GenericRecordRequest> recordRequests,
            List<GenericFabricRecord> records, long count, int type) {
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
            GenericFabricRecord fabricRecord = new GenericFabricRecord();
            fabricRecord.setGenericRecord(builder.build());
            records.add(fabricRecord);

            GenericRecordRequest recordRequest = new GenericRecordRequest();
            String testFile = "testGenericFile" + type;
            recordRequest.setBatchId(id).setCustomerSpace("generic").setStores(Arrays.asList("HDFS"))
                    .setRepositories(Arrays.asList(testFile)).setId(i + "");
            recordRequests.add(recordRequest);
        }

        return records;
    }

    private Schema buildSchem1() {
        String schemaString = "{\"namespace\": \"FabricGenericRecord\", \"type\": \"record\", "
                + "\"name\": \"FabricGenericRecord1\"," + "\"fields\": ["
                + "{\"name\": \"RowId\", \"type\": \"long\"}," + "{\"name\": \"Id\", \"type\": \"string\"},"
                + "{\"name\": \"FirstLastName\", \"type\": \"string\"}" + "]}";

        System.out.println(schemaString);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private Schema buildSchem2() {
        String schemaString = "{\"namespace\": \"FabricGenericRecord\", \"type\": \"record\", "
                + "\"name\": \"FabricGenericRecord2\"," + "\"fields\": ["
                + "{\"name\": \"RowId\", \"type\": \"long\"}," + "{\"name\": \"Id\", \"type\": \"string\"},"
                + "{\"name\": \"FirstLastName\", \"type\": \"string\"}," + "{\"name\": \"age\", \"type\": \"int\"}"
                + "]}";

        System.out.println(schemaString);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }
}
