package com.latticeengines.datafabric.connector.generic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricMessageManager;
import com.latticeengines.datafabric.entitymanager.impl.SampleEntity;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class GenericSinkTaskFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GenericSinkTaskFunctionalTestNG.class);

    @Value("datafabric.message.stack")
    private String stack;

    @Value("datafabric.message.zkConnect")
    private String zkConnect;

    @Resource(name = "genericFabricMessageManager")
    private GenericFabricMessageManager<SampleEntity> entityManager;

    private Schema keySchema;
    private Schema valueSchema;
    private String name;

    private GenericSinkTask task;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {

        task = new GenericSinkTask();
        Map<String, String> props = new HashMap<>();
        props.put(GenericSinkConnectorConfig.STACK.getKey(), stack);
        props.put(GenericSinkConnectorConfig.KAFKA_ZKCONNECT.getKey(), zkConnect);
        props.put(GenericSinkConnectorConfig.HDFS_BASE_DIR.getKey(), BASE_DIR);

        task.start(props);
        HDFSProcessorAdapter adapter = (HDFSProcessorAdapter) ProcessorAdapterFactory.getAdapter(FabricStoreEnum.HDFS,
                task.connectorConfig);
        adapter.setYarnConfig(conf);
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        task.close(null);
        if (HdfsUtils.fileExists(conf, BASE_DIR)) {
            HdfsUtils.rmdir(conf, BASE_DIR);
        }
        if (name != null) {
            entityManager.cleanup(name);
        }
        CamilleEnvironment.stop();
    }

    @Test(groups = "functional", enabled = true)
    public void put() throws Exception {
        name = entityManager.createOrGetNamedBatchId("connectors", null, true);
        task.open(null);
        Collection<SinkRecord> records = new ArrayList<>();

        Struct keyRecord = getKeyRecord();
        Struct valueRecord = getValueRecord();
        SinkRecord record = new SinkRecord("topic", 0, keySchema, keyRecord, valueSchema, valueRecord, 0l);
        records.add(record);
        task.put(records);

        List<GenericRecord> writtenRecords = AvroUtils.getDataFromGlob(conf,
                BASE_DIR + "/connectorFile/Snapshot/*/*.avro");
        Assert.assertEquals(writtenRecords.size(), 1);
        Assert.assertEquals(writtenRecords.get(0).get("LastName").toString(), "Smith");

        Assert.assertEquals(task.version(), "unknown");
        task.flush(null);
    }

    protected static Struct getKeyRecord() {
        GenericRecordRequest recordRequest = new GenericRecordRequest();

        List<FabricStoreEnum> stores = Arrays.asList(FabricStoreEnum.HDFS);
        List<String> repositories = Arrays.asList("connectorFile");
        recordRequest.setBatchId("connectors").setCustomerSpace("generic").setStores(stores)
                .setRepositories(repositories).setId("1");

        Schema keySchema = SchemaBuilder.struct().name("Person").field(GenericRecordRequest.REQUEST_KEY,
                Schema.STRING_SCHEMA);
        Struct struct = new Struct(keySchema).put(GenericRecordRequest.REQUEST_KEY, JsonUtils.serialize(recordRequest));
        return struct;

    }

    protected static Struct getValueRecord() {
        Schema valueSchema = SchemaBuilder.struct().name("Person").field("LastName", Schema.STRING_SCHEMA);
        Struct struct = new Struct(valueSchema).put("LastName", "Smith");
        return struct;

    }
}
