package com.latticeengines.eai.dynamodb.runtime;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.latticeengines.aws.dynamo.impl.DynamoServiceImpl;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class DynamoExportMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportMapper.class);
    private static final int BUFFER_SIZE = 100; // FIXME - change back to 25 if any impact

    private String recordType;
    private String repo;
    private DynamoDataStoreImpl dataStore;
    private Class<?> entityClass;
    private Map<String, Pair<GenericRecord, Map<String, Object>>> recordBuffer = new HashMap<>();
    private AmazonDynamoDB client;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected AvroRowHandler initialize(
            Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable>.Context context,
            Schema schema) {
        recordType = config.get(HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE);
        repo = config.get(HdfsToDynamoConfiguration.CONFIG_REPOSITORY);

        log.info("recordType=" + recordType);
        log.info("repo=" + repo);

        String endpoint = config.get(HdfsToDynamoConfiguration.CONFIG_ENDPOINT);
        String region = config.get(HdfsToDynamoConfiguration.CONFIG_AWS_REGION);
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Instantiate AmazonDynamoDBClient using endpoint " + endpoint);
            client = AmazonDynamoDBClientBuilder.standard() //
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)) //
                    .build();
        } else {
            String accessKey = CipherUtils
                    .decrypt(config.get(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED))
                    .replace("\n", "");
            String secretKey = CipherUtils
                    .decrypt(config.get(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED)).replace("\n", "");
            log.info("Instantiate AmazonDynamoDBClient using BasicAWSCredentials");
            AWSCredentialsProvider credsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey));
            client = AmazonDynamoDBClientBuilder.standard().withCredentials(credsProvider) //
                    .withRegion(Regions.fromName(region)) //
                    .build();
        }

        resolveEntityClass();
        constructDataStore();

        return this;
    }

    @Override
    protected void finalize(
            Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable>.Context context) {
        if (!recordBuffer.isEmpty()) {
            commitBuffer(context.getCounter(RecordExportCounter.EXPORTED_RECORDS),
                    context.getCounter(RecordExportCounter.ERROR_RECORDS));
        }
    }

    @Override
    public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context) {
        context.getCounter(RecordExportCounter.SCANNED_RECORDS).increment(1);

        long totalCount = context.getCounter(RecordExportCounter.SCANNED_RECORDS).getValue();
        if (totalCount % 10000L == 0) {
            log.info("Already scanned " + totalCount + " records.");
        }

        try {
            GenericData.Record record = key.datum();
            loadToBuffer(record);
        } catch (Exception e) {
            context.getCounter(RecordExportCounter.ERROR_RECORDS).increment(1);
            log.error("Failed to load record to buffer: " + key.datum(), e);
        }

        if (recordBuffer.size() >= BUFFER_SIZE) {
            commitBuffer(context.getCounter(RecordExportCounter.EXPORTED_RECORDS),
                    context.getCounter(RecordExportCounter.ERROR_RECORDS));
        }
    }

    @Override
    public void startRecord(GenericData.Record record) {
    }

    @Override
    public void handleField(GenericData.Record record, Schema.Field field) {
    }

    @Override
    public void endRecord(GenericData.Record record) {
    }

    private void loadToBuffer(GenericRecord record) {
        FabricEntity<?> entity;
        if (GenericTableEntity.class.equals(entityClass)) {
            String keyPrefix = config.get(HdfsToDynamoConfiguration.CONFIG_KEY_PREFIX);
            String parititionKey = config.get(HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY);
            String sortKey = config.get(HdfsToDynamoConfiguration.CONFIG_SORT_KEY);
            entity = new GenericTableEntity(keyPrefix, parititionKey, sortKey).fromHdfsAvroRecord(record);
        } else if (GenericTableActivity.class.equals(entityClass)) {
            String parititionKey = config.get(HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY);
            String sortKey = config.get(HdfsToDynamoConfiguration.CONFIG_SORT_KEY);
            entity = new GenericTableActivity(parititionKey, sortKey).fromHdfsAvroRecord(record);
        } else {
            entity = (FabricEntity<?>) FabricEntityFactory.fromHdfsAvroRecord(record, entityClass);
        }
        GenericRecord mbusRecord = entity.toFabricAvroRecord(recordType);
        Map<String, Object> tags = entity.getTags();
        recordBuffer.put(entity.getId(), Pair.of(mbusRecord, tags));
    }

    private void commitBuffer(Counter whiteCounter, Counter blackCounter) {
        try {
            dataStore.createRecords(recordBuffer);
            whiteCounter.increment(recordBuffer.size());
        } catch (Exception e) {
            blackCounter.increment(recordBuffer.size());
            log.error("Failed to commit a buffer of size " + recordBuffer.size() + ". Total failed  = "
                    + blackCounter.getValue(), e);
            recordBuffer.values().forEach(pair -> log.info(pair.getLeft().toString()));
        } finally {
            recordBuffer.clear();
        }
    }

    private void resolveEntityClass() {
        try {
            entityClass = Class.forName(config.get(HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Cannot find the entity class: " + config.get(HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME));
        }
        log.info("Entity Class: " + entityClass);

        if (!FabricEntity.class.isAssignableFrom(entityClass)) {
            throw new RuntimeException(
                    "Entity Class " + entityClass.getSimpleName() + " does implement the FabricEntity interface.");
        }
    }

    private void constructDataStore() {
        Schema fabricSchema = FabricEntityFactory.getFabricSchema(entityClass, recordType);

        String dynamoProp = DynamoUtil.constructIndex(entityClass);
        if (dynamoProp != null) {
            fabricSchema.addProp(DynamoUtil.KEYS, dynamoProp);
        }
        dynamoProp = DynamoUtil.constructAttributes(entityClass);
        if (dynamoProp != null) {
            fabricSchema.addProp(DynamoUtil.ATTRIBUTES, dynamoProp);
        }
        log.info("Fabric schema: " + fabricSchema.toString(true));
        dataStore = new DynamoDataStoreImpl(new DynamoServiceImpl(client), repo, recordType, fabricSchema);
    }

}
