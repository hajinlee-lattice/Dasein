package com.latticeengines.datafabric.connector.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;

import com.amazonaws.auth.BasicAWSCredentials;
import com.latticeengines.aws.dynamo.impl.DynamoServiceImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.datafabric.service.datastore.impl.FabricDataServiceImpl;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class DynamoProcessorAdapter extends AbstractProcessorAdapter {

    private final Logger log = LoggerFactory.getLogger(DynamoProcessorAdapter.class);

    private GenericSinkConnectorConfig connectorConfig;
    private FabricDataStore dataStore;

    @Override
    public void setup(GenericSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public int write(String repository, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs) {

        int count = 0;
        DynamoDataServiceProvider dynamoProvider = new DynamoDataServiceProvider();
        getDataStore(dynamoProvider, repository, pairs);
        List<Pair<GenericRecordRequest, GenericRecord>> allPairs = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> entry : pairs.entrySet()) {
            allPairs.addAll(entry.getValue());
            count += entry.getValue().size();
        }
        addBatchs(allPairs);
        log.info("Wrote generic connector records, count=" + count + " store=" + dynamoProvider.getName()
                + " repository=" + repository);
        return count;
    }

    private void addBatchs(List<Pair<GenericRecordRequest, GenericRecord>> allPairs) {
        int batchSize = 25;
        List<Pair<GenericRecordRequest, GenericRecord>> batch = new ArrayList<>();
        for (Pair<GenericRecordRequest, GenericRecord> pair : allPairs) {
            batch.add(pair);
            if (batch.size() >= batchSize) {
                dataStore.createRecords(getPairMap(batch));
                batchSize = 0;
                batch.clear();
            }
        }
        if (batch.size() > 0) {
            dataStore.createRecords(getPairMap(batch));
        }
    }

    private void getDataStore(DynamoDataServiceProvider dynamoProvider, String repository,
            Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs) {
        if (dataStore == null) {
            synchronized (this) {
                if (dataStore == null) {
                    String accessKey = connectorConfig.getProperty(GenericSinkConnectorConfig.ACCESS_KEY, String.class);
                    String secretKey = connectorConfig.getProperty(GenericSinkConnectorConfig.SECRET_KEY, String.class);
                    log.info("access key=" + accessKey + " secret key=" + secretKey);
                    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
                    String endpoint = null;
                    DynamoServiceImpl dynamoService = new DynamoServiceImpl(awsCredentials, endpoint);

                    dynamoProvider.setDynamoService(dynamoService);
                    FabricDataService dataService = new FabricDataServiceImpl();
                    dataService.addServiceProvider(dynamoProvider);

                    Pair<GenericRecordRequest, GenericRecord> pair = pairs.values().iterator().next().get(0);
                    dataStore = dataService.constructDataStore(dynamoProvider.getName(), repository, pair.getKey()
                            .getRecordType(), pair.getValue().getSchema());
                    ((DynamoDataStoreImpl) dataStore).useRemoteDynamo(true);
                }
            }
        }
    }

}
