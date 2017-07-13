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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.aws.dynamo.impl.DynamoServiceImpl;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class DynamoExportMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportMapper.class);
    private static final int BUFFER_SIZE = 25;

    private String recordType;
    private String repo;
    private DynamoDataStoreImpl dataStore;
    private Class<?> entityClass;
    private Map<String, Pair<GenericRecord, Map<String, Object>>> recordBuffer = new HashMap<>();
    private AmazonDynamoDBClient client;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected AvroRowHandler initialize(
            Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable>.Context context,
            Schema schema) throws IOException, InterruptedException {
        Table table = JsonUtils.deserialize(config.get("eai.table.schema"), Table.class);
        String indented = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(table);
        log.info("Table:\n" + indented);

        recordType = config.get(DynamoExportJob.CONFIG_RECORD_TYPE);
        repo = config.get(DynamoExportJob.CONFIG_REPOSITORY);

        log.info("recordType=" + recordType);
        log.info("repo=" + repo);

        String endpoint = config.get(DynamoExportJob.CONFIG_ENDPOINT);
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Instantiate AmazonDynamoDBClient using endpoint " + endpoint);
            client = new AmazonDynamoDBClient().withEndpoint(endpoint);
        } else {
            String accessKey = CipherUtils.decrypt(config.get(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED))
                    .replace("\n", "");
            String secretKey = CipherUtils.decrypt(config.get(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED))
                    .replace("\n", "");
            log.info("Instantiate AmazonDynamoDBClient using BasicAWSCredentials");
            client = new AmazonDynamoDBClient(new BasicAWSCredentials(accessKey, secretKey));
        }

        resolveEntityClass();
        constructDataStore();

        return this;
    }

    @Override
    protected void finalize(
            Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        if (!recordBuffer.isEmpty()) {
            commitBuffer(context.getCounter(RecordExportCounter.EXPORTED_RECORDS),
                    context.getCounter(RecordExportCounter.ERROR_RECORDS));
        }
    }

    @Override
    public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
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
    public void startRecord(GenericData.Record record) throws IOException {
    }

    @Override
    public void handleField(GenericData.Record record, Schema.Field field) throws IOException {
    }

    @Override
    public void endRecord(GenericData.Record record) throws IOException {
    }

    private void loadToBuffer(GenericRecord record) {
        FabricEntity<?> entity = (FabricEntity<?>) FabricEntityFactory.fromHdfsAvroRecord(record, entityClass);
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
        } finally {
            recordBuffer.clear();
        }
    }

    private void resolveEntityClass() {
        try {
            entityClass = Class.forName(config.get(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Cannot find the entity class: " + config.get(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME));
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

        dataStore = new DynamoDataStoreImpl(new DynamoServiceImpl(client), repo, recordType, fabricSchema);
    }

}
