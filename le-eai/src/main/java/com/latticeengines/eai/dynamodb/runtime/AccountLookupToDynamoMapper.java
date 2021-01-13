package com.latticeengines.eai.dynamodb.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
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
import com.latticeengines.domain.exposed.cdl.DynamoAccountLookupRecord;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class AccountLookupToDynamoMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(AccountLookupToDynamoMapper.class);

    private static final int BUFFER_SIZE = 100; // FIXME - changed back to 25 if any impact
    private static final String ATTR_PARTITION_KEY = InterfaceName.AtlasLookupKey.name();

    private static final String KEY_FORMAT = "AccountLookup__%s__%s__%s"; // tenantId__ver__<lookupId_lookIdVal>

    private static final String AccountId = "AccountId";
    private static final String CHANGED = "__Changed__";
    private static final String DELETED = "__Deleted__";
    private static final String TTLAttr = "EXP";
    private static final String SortKey = "SortKey"; // dummy

    private String tenant;
    private String tableName;
    private Integer targetVersion = 0;
    private final Set<String> lookupIds = new HashSet<>();
    private final List<DynamoAccountLookupRecord> recordBuffer = new ArrayList<>();
    private DynamoItemService dynamoItemService;
    private long expTime;

    @Override
    protected AvroRowHandler initialize(Context context, Schema schema) throws IOException, InterruptedException {
        tenant = config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_TENANT);
        tableName = config.get(HdfsToDynamoConfiguration.CONFIG_TABLE_NAME);
        log.info("tenant=" + tenant);
        log.info("tableName=" + tableName);

        String lookupIdsStr = config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS);
        if (StringUtils.isNotBlank(lookupIdsStr)) {
            lookupIds.addAll(Arrays.asList(lookupIdsStr.split(",")));
        }
        lookupIds.add(InterfaceName.AccountId.name());
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
                    .decrypt(config.get(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED)).replace("\n", "");
            log.info("Instantiate AmazonDynamoDBClient using BasicAWSCredentials");
            AWSCredentialsProvider credsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey));
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(credsProvider) //
                    .withRegion(Regions.fromName(region)) //
                    .build();
            dynamoItemService = new DynamoItemServiceImpl(new DynamoDB(client));
        }
        targetVersion = Integer.parseInt(config.get(HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION));
        expTime = Long.parseLong(config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL));

        return this;
    }

    @Override
    public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
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

    private void loadToBuffer(GenericRecord record) {
        String accountId = record.get(InterfaceName.AccountId.name()).toString();
        String lookupEntry = record.get(InterfaceName.AtlasLookupKey.name()).toString(); // <lookupId>_<lookupVal>
        if (StringUtils.isBlank(lookupEntry)) {
            return;
        }
        String key = String.format(KEY_FORMAT, tenant, targetVersion, lookupEntry);
        if (StringUtils.isBlank(key)) {
            return;
        }
        recordBuffer.add((new DynamoAccountLookupRecord.Builder()).key(key).accountId(accountId).build());
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

    private List<Item> convertRecordBufferToItems(List<DynamoAccountLookupRecord> records) {
        List<Item> items = new ArrayList<>();
        for (DynamoAccountLookupRecord record : records) {
            PrimaryKey key = new PrimaryKey(ATTR_PARTITION_KEY, record.key);
            Item item = new Item() //
                    .withPrimaryKey(key) //
                    .withString(AccountId, record.accountId) //
                    .withBoolean(CHANGED, record.changed) //
                    .withBoolean(DELETED, record.deleted) //
                    .withLong(TTLAttr, expTime) //
                    .withInt(SortKey, 0);
            items.add(item);
        }
        return items;
    }

    @Override
    protected void finalize(Context context) throws IOException, InterruptedException {
        if (!recordBuffer.isEmpty()) {
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
}
