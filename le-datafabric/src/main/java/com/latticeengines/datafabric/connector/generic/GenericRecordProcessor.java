package com.latticeengines.datafabric.connector.generic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class GenericRecordProcessor {

    private static final Log log = LogFactory.getLog(GenericRecordProcessor.class);

    private GenericSinkConnectorConfig connectorConfig;
    private GenericFabricEntityManager entityManager;

    List<GenericRecord> allKeyRecords = new ArrayList<>();
    List<GenericRecord> allValueRecords = new ArrayList<>();
    private List<TopicPartition> allTopicPartitions = new ArrayList<>();

    Set<String> repositorySet = new HashSet<>();

    Map<String, Long> batchCountMap = new HashMap<>();

    public GenericRecordProcessor(GenericSinkConnectorConfig connectorConfig, GenericFabricEntityManager entityManager) {
        this.connectorConfig = connectorConfig;
        this.entityManager = entityManager;

        String repositories = connectorConfig.getProperty(GenericSinkConnectorConfig.REPOSITORIES, String.class);
        String[] tokens = repositories.split(";");
        repositorySet.addAll(Arrays.asList(tokens));

    }

    public void addAll(List<GenericRecord> keyRecords, List<GenericRecord> valueRecords,
            List<TopicPartition> topicPartitions) {

        allKeyRecords.addAll(keyRecords);
        allValueRecords.addAll(valueRecords);
        allTopicPartitions.addAll(topicPartitions);
    }

    public void process() {
        Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> recordMap = populateRecords();
        writeRecords(recordMap);
        updateRecordCount();
    }

    private void updateRecordCount() {
        if (batchCountMap.size() == 0) {
            return;
        }
        for (Map.Entry<String, Long> entry : batchCountMap.entrySet()) {
            entityManager.updateBatchCount(entry.getKey(), entry.getValue());
        }
    }

    private void writeRecords(
            Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> recordMap) {

        for (Map.Entry<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> storeEntry : recordMap
                .entrySet()) {
            String store = storeEntry.getKey();
            if (!repositorySet.contains(store)) {
                log.warn("The store is not supported, store=" + store);
                continue;
            }
            ProcessorAdapter adapter = ProcessorAdapterFactory.getAdapter(store, connectorConfig);
            Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>> repositoryMap = storeEntry
                    .getValue();
            for (Map.Entry<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>> repositoryEntry : repositoryMap
                    .entrySet()) {
                String repository = repositoryEntry.getKey();
                adapter.write(repository, repositoryEntry.getValue());
            }
        }
    }

    private Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> populateRecords() {
        Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> recordMap = new HashMap<>();
        for (int i = 0; i < allKeyRecords.size(); i++) {
            GenericRecord keyRecord = allKeyRecords.get(i);
            GenericRecord valueRecord = allValueRecords.get(i);
            TopicPartition topicPartition = allTopicPartitions.get(i);
            populateRecordPerRecord(recordMap, keyRecord, valueRecord, topicPartition);
        }
        log.info("Populated generic connector records, count=" + allKeyRecords.size());
        return recordMap;
    }

    private void populateRecordPerRecord(
            Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> recordMap,
            GenericRecord keyRecord, GenericRecord valueRecord, TopicPartition topicPartition) {
        GenericRecordRequest request = JsonUtils.deserialize((String) keyRecord.get(GenericRecordRequest.REQUEST_KEY),
                GenericRecordRequest.class);
        List<String> stores = request.getStores();
        List<String> repositories = request.getRepositories();
        for (int j = 0; j < stores.size(); j++) {
            String store = stores.get(j);
            String repository = repositories.get(j);
            populateRecordPerRepository(recordMap, valueRecord, topicPartition, request, store, repository);
        }
    }

    private void populateRecordPerRepository(
            Map<String, Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>> recordMap,
            GenericRecord valueRecord, TopicPartition topicPartition, GenericRecordRequest request, String store,
            String repository) {
        if (!recordMap.containsKey(store)) {
            recordMap.put(store,
                    new HashMap<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>>());
        }
        Map<String, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>> repositoryMap = recordMap
                .get(store);
        if (!repositoryMap.containsKey(repository)) {
            repositoryMap.put(repository,
                    new HashMap<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>>());
        }
        Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> partitionMap = repositoryMap
                .get(repository);
        if (!partitionMap.containsKey(topicPartition)) {
            partitionMap.put(topicPartition, new ArrayList<Pair<GenericRecordRequest, GenericRecord>>());
        }
        partitionMap.get(topicPartition).add(Pair.of(request, valueRecord));

        countRecords(request);
    }

    private void countRecords(GenericRecordRequest request) {
        if (!batchCountMap.containsKey(request.getBatchId())) {
            batchCountMap.put(request.getBatchId(), new Long(1));
        } else {
            batchCountMap.put(request.getBatchId(), batchCountMap.get(request.getBatchId()) + 1);
        }
    }
}
