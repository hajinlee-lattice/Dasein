package com.latticeengines.datafabric.connector.generic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class DynamoProcessorAdapterUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DynamoProcessorAdapterUnitTestNG.class);

    private DynamoProcessorAdapter adapter;
    private Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> map;
    private GenericRecord record;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        adapter = new DynamoProcessorAdapter();

    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
    }

    @Test(groups = "unit", enabled = true)
    public void getDataStore() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GenericSinkConnectorConfig.ACCESS_KEY.getKey(), "access");
        props.put(GenericSinkConnectorConfig.SECRET_KEY.getKey(), "secret");
        GenericSinkConnectorConfig config = new GenericSinkConnectorConfig(props);
        adapter.setup(config);
        DynamoDataServiceProvider dynamoProvider = new DynamoDataServiceProvider();
        map = new HashMap<>();
        TopicPartition partition = new TopicPartition("topic", 0);

        record = getRecord();
        map.put(partition, Arrays.asList(Pair.of(getRecordRequest(), record)));
        adapter.getDataStore(dynamoProvider, "repository", map);
        DynamoDataStoreImpl dataStore = (DynamoDataStoreImpl) adapter.getDataStore();
        Assert.assertEquals(ReflectionTestUtils.getField(dataStore, "repository"), "repository");

    }

    @Test(groups = "unit", enabled = true, dependsOnMethods = "getDataStore")
    public void write() throws Exception {
        FabricDataStore store = mock(FabricDataStore.class);
        final List<Map<String, Pair<GenericRecord, Map<String, Object>>>> result = new ArrayList<>();
        doAnswer(new Answer<Object>() {
            @SuppressWarnings("unchecked")
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] params = invocation.getArguments();
                result.add((Map<String, Pair<GenericRecord, Map<String, Object>>>) params[0]);
                return null;
            }
        }).when(store).createRecords(any());
        adapter.setDataStore(store);
        adapter.write("repository", map);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).get("1").getLeft(), record);

    }

    protected GenericRecordRequest getRecordRequest() {
        GenericRecordRequest recordRequest = new GenericRecordRequest();
        String testFile = "testGenericFile";
        List<FabricStoreEnum> stores = Arrays.asList(FabricStoreEnum.DYNAMO);
        List<String> repositories = Arrays.asList(testFile);
        recordRequest.setBatchId("batchId").setCustomerSpace("generic").setStores(stores).setRepositories(repositories)
                .setId("1");
        return recordRequest;
    }

    protected GenericRecord getRecord() {
        String schemaString = "{\"namespace\": \"FabricGenericRecord\", \"type\": \"record\", "
                + "\"name\": \"FabricGenericRecord1\"," + "\"fields\": ["
                + "{\"name\": \"FirstName\", \"type\": \"string\"}" + "]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("FirstName", "Mary");
        return builder.build();
    }
}
