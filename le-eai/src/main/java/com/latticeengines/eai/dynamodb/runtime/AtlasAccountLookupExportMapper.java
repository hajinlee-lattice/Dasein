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
import com.latticeengines.domain.exposed.cdl.DynamoAccountLookupRecord;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordExportCounter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.eai.runtime.mapreduce.AvroExportMapper;
import com.latticeengines.eai.runtime.mapreduce.AvroRowHandler;

public class AtlasAccountLookupExportMapper extends AvroExportMapper implements AvroRowHandler {

    private static final Logger log = LoggerFactory.getLogger(AtlasAccountLookupExportMapper.class);

    private static final int BUFFER_SIZE = 100; // FIXME - changed back to 25 if any impact
    private static final String ATTR_PARTITION_KEY = InterfaceName.AtlasLookupKey.name();
    private static final String ATTR_ACCOUNT_ID = "AccountId";

    private static final String KEY_FORMAT = "AccountLookup__%s__%s__%s_%s"; // tenantId__ver__lookupId_lookIdVal

    private static final String CHANGELIST_ACCOUNT_ID = "RowId";
    private static final String CHANGELIST_LOOKUP_NAME = "ColumnId";
    private static final String CHANGELIST_FROM_STRING = "FromString";
    private static final String CHANGELIST_TO_STRING = "ToString";
    private static final String CHANGELIST_DELETED = "Deleted";
    private static final String CHANGED = "__Changed__";
    private static final String DELETED = "__Deleted__";
    private static final String TTLAttr = "EXP";
    private static final String SortKey = "SortKey"; // dummy

    private String tenant;
    private String tableName;
    private Integer currentVersion = 0;
    private final Set<String> lookupIds = new HashSet<>();
    private final List<DynamoAccountLookupRecord> recordBuffer = new ArrayList<>();
    private DynamoItemService dynamoItemService;
    private long expTime;

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
        lookupIds.add(InterfaceName.AccountId.name());
        lookupIds.add(InterfaceName.CustomerAccountId.name());
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
        currentVersion = parseTargetVersion(config.get(HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION));
        expTime = Long.parseLong(config.get(HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL));

        return this;
    }

    private Integer parseTargetVersion(String targetVersion) {
        return StringUtils.isBlank(targetVersion) ? 0 : Integer.parseInt(targetVersion);
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
        if (lookupIdModified(record)) {
            String lookupId = record.get(CHANGELIST_LOOKUP_NAME).toString();
            if (lookupIdCreated(record)) {
                String lookupIdVal = record.get(CHANGELIST_TO_STRING).toString();
                String key = constructLookupKey(tenant, lookupId, lookupIdVal);
                recordBuffer.add((new DynamoAccountLookupRecord.Builder()) //
                        .key(key) //
                        .accountId(record.get(CHANGELIST_ACCOUNT_ID).toString()) //
                        .build() //
                );
            } else if (lookupIdChanged(record)) {
                // mark accountId_oldLookupId record deleted, create and mark
                // accountId_newLookupId changed
                String oldLookupIdVal = record.get(CHANGELIST_FROM_STRING).toString();
                String oldKey = constructLookupKey(tenant, lookupId, oldLookupIdVal);
                recordBuffer.add((new DynamoAccountLookupRecord.Builder()) //
                        .key(oldKey) //
                        .accountId(record.get(CHANGELIST_ACCOUNT_ID).toString()) //
                        .deleted(true) //
                        .build() //
                );
                String newLookupIdVal = record.get(CHANGELIST_TO_STRING).toString();
                String newKey = constructLookupKey(tenant, lookupId, newLookupIdVal);
                recordBuffer.add((new DynamoAccountLookupRecord.Builder()) //
                        .key(newKey) //
                        .accountId(record.get(CHANGELIST_ACCOUNT_ID).toString()) //
                        .changed(true) //
                        .build() //
                );
            } else if (lookupIdDeleted(record)) {
                // mark accountId_lookupId record deleted
                String lookupIdVal = record.get(CHANGELIST_FROM_STRING).toString();
                String key = constructLookupKey(tenant, lookupId, lookupIdVal);
                recordBuffer.add((new DynamoAccountLookupRecord.Builder()) //
                        .key(key) //
                        .accountId(record.get(CHANGELIST_ACCOUNT_ID).toString()) //
                        .deleted(true) //
                        .build() //
                );
            }
        }
    }

    private boolean lookupIdModified(GenericRecord record) {
        return record.get(CHANGELIST_ACCOUNT_ID) != null && record.get(CHANGELIST_LOOKUP_NAME) != null
                && lookupIds.contains(record.get(CHANGELIST_LOOKUP_NAME).toString());
    }

    private boolean lookupIdCreated(GenericRecord record) {
        return record.get(CHANGELIST_FROM_STRING) == null && record.get(CHANGELIST_TO_STRING) != null;
    }

    private boolean lookupIdChanged(GenericRecord record) {
        return record.get(CHANGELIST_FROM_STRING) != null && record.get(CHANGELIST_TO_STRING) != null;
    }

    private boolean lookupIdDeleted(GenericRecord record) {
        return record.get(CHANGELIST_DELETED) != null
                && Boolean.parseBoolean(record.get(CHANGELIST_DELETED).toString());
    }

    private String constructLookupKey(String tenant, String lookupId, String lookupVal) {
        return String.format(KEY_FORMAT, tenant, currentVersion, lookupId, lookupVal.toLowerCase());
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
                    .withString(ATTR_ACCOUNT_ID, record.accountId) //
                    .withBoolean(CHANGED, record.changed) //
                    .withBoolean(DELETED, record.deleted) //
                    .withLong(TTLAttr, expTime) //
                    .withInt(SortKey, 0);
            items.add(item);
        }
        return items;
    }
}
