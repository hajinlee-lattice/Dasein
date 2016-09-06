package com.latticeengines.eai.dynamodb.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final Log log = LogFactory.getLog(DynamoExportMapper.class);
    private static final int BUFFER_SIZE = 25;
    private static final int MAX_RETRIES = 10;

    private String recordType;
    private String repo;
    private DynamoDataStoreImpl dataStore;
    private Class<?> entityClass;
    private Map<String, GenericRecord> recordBuffer = new HashMap<>();
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
        try {
            GenericData.Record record = key.datum();
            loadToBuffer(record);
        } catch (Exception e) {
            context.getCounter(RecordExportCounter.ERROR_RECORDS).increment(1);
            log.error("Failed load record to buffer: " + key.datum(), e);
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
        recordBuffer.put(entity.getId(), mbusRecord);
    }

    private void commitBuffer(Counter whiteBuffer, Counter blackBuffer) {
        int originalSize = recordBuffer.size();
        int retry = 0;
        int interval = 100;
        Random random = new Random(System.currentTimeMillis());

        while (retry++ < MAX_RETRIES && !recordBuffer.isEmpty()) {
            try {
                if (retry > 1) {
                    log.info("Attempt to commit record buffer (Attempt=" + retry + "). BufferSize="
                            + recordBuffer.size());
                }
                attemptCommitBuffer(whiteBuffer);
                Thread.sleep(interval);
            } catch (Exception e) {
                log.warn("Attempt to commit buffer failed. (Attempt=" + retry + ")", e);
            } finally {
                interval = interval + random.nextInt(interval);
            }
        }

        int finalSize = recordBuffer.size();
        log.info("Committed " + (originalSize - finalSize) + " records to DynamoDB. Total committed  = "
                + whiteBuffer.getValue());
        if (!recordBuffer.isEmpty()) {
            log.error(
                    "Failed to commit " + recordBuffer.size() + " records. Total failed  = " + blackBuffer.getValue());
        }

        recordBuffer.clear();
    }

    private void attemptCommitBuffer(Counter whiteBuffer) {
        try {
            dataStore.createRecords(recordBuffer);
        } catch (Exception e) {
            log.error("Error when committing buffer.", e);
        }

        List<String> ids = new ArrayList<>(recordBuffer.keySet());
        for (String id : ids) {
            GenericRecord record = null;
            try {
                record = dataStore.findRecord(id);
            } catch (Exception e) {
                log.error("Error when finding record of id " + id);
            }
            if (record != null) {
                recordBuffer.remove(id);
                whiteBuffer.increment(1);
            }
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
        // get the reflected schema for Entity
        Schema fabricSchema = FabricEntityFactory.getFabricSchema(entityClass, recordType);

        // add dynamo attributes
        String dynamoProp = DynamoUtil.constructAttributes(entityClass);
        if (dynamoProp != null) {
            fabricSchema.addProp(DynamoUtil.ATTRIBUTES, dynamoProp);
        }

        dataStore = new DynamoDataStoreImpl(client, repo, recordType, fabricSchema);
    }

}
