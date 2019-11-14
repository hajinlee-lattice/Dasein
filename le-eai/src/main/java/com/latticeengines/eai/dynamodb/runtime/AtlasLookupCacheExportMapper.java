package com.latticeengines.eai.dynamodb.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.impl.DynamoItemServiceImpl;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class AtlasLookupCacheExportMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportMapper.class);

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(60);
    private static final int BUFFER_SIZE = 25;
    private static final String ATTR_PARTITION_KEY = "Key";
    private static final String ATTR_ACCOUNT_ID = "AccountId";

    private String recordType;
    private String repo;
    private DynamoDataStoreImpl dataStore;
    private Class<?> entityClass;

    private String tenant;
    private String tableName;
    private List<String> lookupIds = new ArrayList<>();
    private List<Pair<String, String>> recordBuffer = new ArrayList<>();
    private DynamoItemService dynamoItemService;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected AvroRowHandler initialize(
            Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable>.Context context,
            Schema schema) {
        tenant = config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_TENANT);
        tableName = config.get(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME);
        log.info("tenant=" + tenant);
        log.info("tableName=" + tableName);

        String lookupIdsStr = config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS);
        if (StringUtils.isNotBlank(lookupIdsStr)) {
            lookupIds.addAll(Arrays.asList(lookupIdsStr.split(",")));
        }
        if (!lookupIds.contains(InterfaceName.AccountId.name())) {
            lookupIds.add(InterfaceName.AccountId.name());
        }
        log.info("lookupIds=[" + StringUtils.join(lookupIds, ",") + "]");

        String endpoint = config.get(HdfsToDynamoConfiguration.CONFIG_ENDPOINT);
        String region = config.get(HdfsToDynamoConfiguration.CONFIG_AWS_REGION);
        if (StringUtils.isNotEmpty(endpoint)) {
            log.info("Instantiate AmazonDynamoDBClient using endpoint " + endpoint);
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard() //
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)) //
                    .build();
            dynamoItemService = new DynamoItemServiceImpl(new DynamoDB(client));
        } else {
            String accessKey = CipherUtils
                    .decrypt(config.get(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED))
                    .replace("\n", "");
            String secretKey = CipherUtils
                    .decrypt(config.get(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED))
                    .replace("\n", "");
            log.info("Instantiate AmazonDynamoDBClient using BasicAWSCredentials");
            AWSCredentialsProvider credsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey));
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(credsProvider) //
                    .withRegion(Regions.fromName(region)) //
                    .build();
            dynamoItemService = new DynamoItemServiceImpl(new DynamoDB(client));
        }

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
        String accountId = record.get(InterfaceName.AccountId.name()).toString();
        for (String lookupId: lookupIds) {
            Object obj = record.get(lookupId);
            if (obj != null) {
                String lookupIdVal = obj.toString();
                String pk = String.format("%s_%s_%s", tenant, lookupId, lookupIdVal.toLowerCase());
                recordBuffer.add(Pair.of(pk, accountId));
                if (!InterfaceName.AccountId.name().equals(lookupId)) {
                    pk = String.format("%s_%s_%s", tenant, lookupId, accountId.toLowerCase());
                    recordBuffer.add(Pair.of(pk, accountId));
                }
            }
        }
    }

    private void commitBuffer(Counter whiteCounter, Counter blackCounter) {
        try {
            List<Item> items = convertRecordBufferToItems(recordBuffer);
            dynamoItemService.batchWrite(tableName, items);
            whiteCounter.increment(recordBuffer.size());
        } catch (Exception e) {
            blackCounter.increment(recordBuffer.size());
            log.error("Failed to commit a buffer of size " + recordBuffer.size() + ". Total failed  = "
                    + blackCounter.getValue(), e);
            recordBuffer.forEach(pair -> log.info(JsonUtils.serialize(pair)));
        } finally {
            recordBuffer.clear();
        }
    }

    private List<Item> convertRecordBufferToItems(List<Pair<String, String>> pairs) {
        List<Item> items = new ArrayList<>();
        for (Pair<String, String> pair: pairs) {
            PrimaryKey key = new PrimaryKey(ATTR_PARTITION_KEY, pair.getKey());
            Item item =  new Item()
                    .withPrimaryKey(key)
                    .withString(ATTR_ACCOUNT_ID, pair.getValue());
            items.add(item);
        }
        return items;
    }

}
