package com.latticeengines.datafabric.service.datastore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xerial.snappy.Snappy;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.domain.exposed.datafabric.DynamoAttributes;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;

public class DynamoDataStoreImpl implements FabricDataStore {

    private static final Log log = LogFactory.getLog(DynamoDataStoreImpl.class);

    private static final String ERRORMESSAGE = "If you see NoSuchMethodError on jackson json, it might be because to the table name or key attributes are wrong.";

    private static final String REPO = "_REPO_";
    private static final String RECORD = "_RECORD_";

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(30);
    private static final int DYNAMODB_BATCH_LIMIT = 100;

    // Default attributes for every table
    public final String ID = "Id";
    private final String BLOB = "Record";

    private String repository;
    private String recordType;
    private Schema schema;
    private DynamoService dynamoService;

    private DynamoIndex tableIndex;
    private DynamoAttributes tableAttributes;
    private String tableName = null;

    public DynamoDataStoreImpl(DynamoService dynamoService, String repository, String recordType, Schema schema) {
        this.dynamoService = dynamoService;
        this.repository = repository;
        this.recordType = recordType;
        this.tableName = buildTableName();
        this.schema = schema;

        String keyProp = schema.getProp(DynamoUtil.KEYS);
        this.tableIndex = DynamoUtil.getIndex(keyProp);

        if (tableIndex == null) {
             tableIndex = new DynamoIndex();
             tableIndex.setHashKeyAttr(ID);
             tableIndex.setHashKeyField(ID);
        }

        String attrProp = schema.getProp(DynamoUtil.ATTRIBUTES);
        this.tableAttributes = DynamoUtil.getAttributes(attrProp);

        log.info("Constructed Dynamo data store repo " + repository + " record " + recordType + " attributes "
                + keyProp);
    }

    @Override
    public void createRecord(String id, GenericRecord record) {

        if (isTimeSeriesStore()) {
            Map<String, GenericRecord> records = new HashMap<String, GenericRecord>();
            records.put(id, record);
            updateBuckets(records);
        } else {
            writeRecord(id, record);
        }
    }

    @Override
    public void createRecords(Map<String, GenericRecord> records) {

        if (isTimeSeriesStore()) {
            updateBuckets(records);
        } else {
            writeRecords(records);
        }
    }

    @Override
    public void deleteRecord(String id, GenericRecord record) {
        DynamoKey dk = constructDynamoKey(id, record);
        deleteRecordByKey(dk);
    }

    public void deleteRecords(Map<String,String> properties) {
        DynamoKey dk= constructDynamoKey(properties);
        if (dk == null) {
            return;
        }
        deleteRecordByKey(dk);
    }

    @Override
    public void updateRecord(String id, GenericRecord record) {

        if (isTimeSeriesStore()) {
            log.info("Timeseries update is not supported");
            return;
        }
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);

        UpdateItemSpec updateItemSpec = buildUpdateItemSpec(id, record);
        if (updateItemSpec == null) {
            return;
        }

        try {
            table.updateItem(updateItemSpec);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to update record " + tableName + " id " + id, e);
        }
    }

    @Override
    public GenericRecord findRecord(String id) {

        DynamoKey dk = constructDynamoKey(id);

        if (dk == null) {
            log.error("Invalid id for this Dynamo table");
            return null;
        }

        GenericRecord record = null;

        if (!isTimeSeriesStore()) {
            DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
            Table table = dynamoDB.getTable(tableName);
            Item item = null;
            try {
                item = table.getItem(dk.getPrimaryKey());
            } catch (NoSuchMethodError e) {
                log.info("The table name is " + tableName);
                log.info("The key is " + id);
                throw new RuntimeException(ERRORMESSAGE, e);
            } catch (Exception e) {
                log.error("Unable to find record " + tableName + " id " + id, e);
            }
            if (item != null) {
                List<GenericRecord> records;
                records = extractRecords(item);
                record = records.get(0);
            }
        } else {
            List<GenericRecord> records = findRecords(dk);
            record = records.get(0);
        }
        return record;
    }

    @Override
    public List<GenericRecord> findRecords(Map<String, String> properties) {
        DynamoKey dk = constructDynamoKey(properties);
        return findRecords(dk);
    }


    private ItemCollection<QueryOutcome> queryByKey(DynamoKey dk) {
        ItemCollection<QueryOutcome> items = null;
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);

        QuerySpec spec = new QuerySpec().withHashKey(tableIndex.getHashKeyAttr(), dk.getHashKey());

        String rangeKey = dk.getRangeKey();
        if (rangeKey != null) {
            spec = spec.withRangeKeyCondition(new RangeKeyCondition(tableIndex.getRangeKeyAttr()).eq(dk.getRangeKey()));
        }

        String bucketKey = dk.getBucketKey();
        if (bucketKey != null) {
            spec = spec.withAttributesToGet(bucketKey);
        }
        try {
            items = table.query(spec);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to find records " + tableName, e);
        }
        return items;

    }

    private List<GenericRecord> findRecords(DynamoKey dk) {

        ItemCollection<QueryOutcome> items = queryByKey(dk);
        if (items == null) {
            return null;
        }

        List<GenericRecord> records = new ArrayList<GenericRecord>();
        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            extractRecords(item, records);
        }

        String stampKey = dk.getStampKey();
        if (stampKey != null) {
            List<GenericRecord> filtered = new ArrayList<GenericRecord>();
            for (GenericRecord record : records) {
                DynamoKey key = constructDynamoKey(record);
                if (key.getStampKey().equals(stampKey)) {
                    filtered.add(record);
                    break;
                }
            }
            records = filtered;
        }

        return records;
    }


    private void deleteRecordByKey(DynamoKey dk) {
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);
        PrimaryKey pk = dk.getPrimaryKey();
        String bucketKey = dk.getBucketKey();
        String stampKey = dk.getStampKey();

        try {
            if (stampKey != null) {
                deleteStamp(pk, bucketKey, stampKey);
            } else if (bucketKey != null) {
                deleteBucket(pk, bucketKey);
            } else {
               table.deleteItem(pk);
            }
        } catch (NoSuchMethodError e) {
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to delete record " + tableName);
        }
    }

    private void deleteStamp(PrimaryKey pk, String bucketKey, String stampKey) {
        return;
    }

    private void deleteBucket(PrimaryKey pk, String bucketKey) {
        return;
    }

    private Set<ByteBuffer> findBucket(DynamoKey dk) {

        Set<ByteBuffer> bucket = null;

        ItemCollection<QueryOutcome> items = queryByKey(dk);
        if (items == null) {
            return null;
        }

        Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            bucket = item.getByteBufferSet(dk.getBucketKey());
            break;
        }

        return bucket;
    }

    private void updateBuckets(Map<String, GenericRecord> records) {

        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);

        Map<String, Map<String, Map<String, Set<ByteBuffer>>>> buckets =
            new HashMap<String, Map<String, Map<String, Set<ByteBuffer>>>>();
        for (Map.Entry<String, GenericRecord> entry : records.entrySet()) {
            GenericRecord record = entry.getValue();
            DynamoKey dk = constructDynamoKey(record);
            Set<ByteBuffer> bucket = null;
            Map<String, Map<String, Set<ByteBuffer>>> rangeBuckets = buckets.get(dk.getHashKey());
            if (rangeBuckets == null) {
                rangeBuckets = new HashMap<String, Map<String, Set<ByteBuffer>>>();
                buckets.put(dk.getHashKey(), rangeBuckets);
            }
            Map<String, Set<ByteBuffer>> attrBuckets = rangeBuckets.get(dk.getRangeKey());
            if (attrBuckets == null) {
                attrBuckets = new HashMap<String, Set<ByteBuffer>>();
                rangeBuckets.put(dk.getRangeKey(), attrBuckets);
            }

            bucket = attrBuckets.get(dk.getBucketKey());
            if (bucket == null) {
                bucket = findBucket(dk);
                if (bucket == null) {
                    bucket = new HashSet<ByteBuffer>();
                }
                attrBuckets.put(dk.getBucketKey(), bucket);
            }
            bucket.add(avroToBytes(record));
        }

        String hashAttr = tableIndex.getHashKeyAttr();
        String rangeAttr = tableIndex.getRangeKeyAttr();

        for (Map.Entry<String, Map<String, Map<String, Set<ByteBuffer>>>> hashEntry : buckets.entrySet()) {
            String hashKey = hashEntry.getKey();
            Map<String, Map<String, Set<ByteBuffer>>> rangeBuckets = hashEntry.getValue();
            for (Map.Entry<String, Map<String, Set<ByteBuffer>>> rangeEntry : rangeBuckets.entrySet()) {
                String rangeKey = rangeEntry.getKey();
                UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(new PrimaryKey(hashAttr, hashKey, rangeAttr, rangeKey));
                Map<String, Set<ByteBuffer>> attrBuckets = rangeEntry.getValue();
                for (Map.Entry<String, Set<ByteBuffer>> attrBucketEntry : attrBuckets.entrySet()) {
                    String bucketKey = attrBucketEntry.getKey();
                    Set<ByteBuffer> bucket = attrBucketEntry.getValue();
                    updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(bucketKey).put(bucket));
                }
                table.updateItem(updateItemSpec);
            }
        }
    }

    private void writeRecord(String id, GenericRecord record) {
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);
        Item item = buildItem(id, record);
        try {
            table.putItem(item);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to save record " + tableName + " id " + id, e);
        }
    }

    private void writeRecords(Map<String, GenericRecord> records) {
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        TableWriteItems writeItems = new TableWriteItems(tableName);

        for (Map.Entry<String, GenericRecord> entry : records.entrySet()) {
            Item item = buildItem(entry.getKey(), entry.getValue());
            writeItems = writeItems.addItemToPut(item);
        }

        int retries = 0;
        long startTime = System.currentTimeMillis();
        long interval = 500L;

        while (System.currentTimeMillis() - startTime < TIMEOUT) {
            try {
                if (retries > 0) {
                    log.info(String.format("Attempt %d to submit batch write item.", retries));
                }
                submitBatchWrite(dynamoDB, writeItems);
                return;
            } catch (NoSuchMethodError e) {
                log.warn(ERRORMESSAGE);
                try {
                    Thread.sleep(interval);
                    interval *= 2;
                    retries++;
                } catch (Exception e1) {
                    log.warn("Failed to sleep. Ignoring the error.");
                }
            }
        }

        throw new RuntimeException("Failed to successfully finish batch write request within timeout.");
    }

    private void submitBatchWrite(DynamoDB dynamoDB, TableWriteItems writeItems) throws NoSuchMethodError {
        try {
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(writeItems);
            do {
                // Check for unprocessed keys which could happen if you exceed
                // provisioned throughput
                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                if (outcome.getUnprocessedItems().size() != 0) {
                    outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                }

            } while (outcome.getUnprocessedItems().size() > 0);
        } catch (Exception e) {
            log.error("Unable to batch create records " + tableName, e);
        }
    }

    @Override
    public Map<String, GenericRecord> batchFindRecord(List<String> idList) {

        if (isTimeSeriesStore()) {
            log.info("Batch find records is not supported for time series store");
            return null;
        }

        Map<String, GenericRecord> records = new HashMap<String, GenericRecord>();

        if (CollectionUtils.isEmpty(idList)) {
            return records;
        }

        // DynamoDB batch API has a limit on number of records that can be
        // passed in a single request. Due to this we need to make chunks of
        // idList if it is more than max size. Each chunk should be less than or
        // equal to the upper limit.

        // find total number of full record loops
        int totalFullLoops = idList.size() / DYNAMODB_BATCH_LIMIT;
        int startIdx = 0;

        for (int idx = 0; idx < totalFullLoops; idx++) {
            // for each full loop iteration call batchFindRecord with
            // DYNAMODB_BATCH_LIMIT records starting from startIdx
            List<String> subList = idList.subList(startIdx, startIdx + DYNAMODB_BATCH_LIMIT);

            batchFindRecord(subList, records);

            // increment startIdx
            startIdx += DYNAMODB_BATCH_LIMIT;
        }

        if (startIdx < idList.size()) {
            // for remaining records call batchFindRecord
            batchFindRecord(idList.subList(startIdx, idList.size()), records);
        }

        return records;
    }

    private void batchFindRecord(List<String> idList, Map<String, GenericRecord> records) {

        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        TableKeysAndAttributes keys = new TableKeysAndAttributes(tableName);

        for (String id : idList) {
            if (id == null)
                continue;
            DynamoKey dk = constructDynamoKey(id);
            if (dk ==  null) {
                continue;
            }
            keys = keys.addPrimaryKey(dk.getPrimaryKey());
        }

        List<PrimaryKey> pKs = keys.getPrimaryKeys();

        if ((pKs == null) || (pKs.size() == 0)) {
            return;
        }

        try {

            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(keys);
            Map<String, KeysAndAttributes> unprocessed = null;

            do {
                List<Item> items = outcome.getTableItems().get(tableName);
                for (Item item : items) {
                    List<GenericRecord> recordsFromItem = extractRecords(item);
                    for (GenericRecord record : recordsFromItem) {
                        records.put(item.getString(ID), record);
                    }
                }

                // Check for unprocessed keys which could happen if you exceed
                // provisioned
                // throughput or reach the limit on response size.
                unprocessed = outcome.getUnprocessedKeys();

                if (!unprocessed.isEmpty()) {
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessed);
                }
            } while (!unprocessed.isEmpty());
        } catch (NoSuchMethodError e) {
            log.info("The table name is " + tableName);
            log.info("The keys are " + idList);
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to batch get records " + tableName, e);
        }
    }

    private ByteBuffer avroToBytes(GenericRecord record) {
        Schema schema = record.getSchema();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(record, encoder);
            encoder.flush();
            output.flush();
            return ByteBuffer.wrap(Snappy.compress(output.toByteArray()));
        } catch (Exception e) {
            log.warn("Exception in encoding generic record.");
            return null;
        }
    }

    private GenericRecord bytesToAvro(ByteBuffer byteBuffer) {
        try {
            ByteBuffer uncompressed = ByteBuffer.wrap(Snappy.uncompress(byteBuffer.array()));
            try (InputStream input = new ByteArrayInputStream(uncompressed.array())) {
                DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                DataInputStream din = new DataInputStream(input);
                Decoder decoder = DecoderFactory.get().binaryDecoder(din, null);
                return reader.read(null, decoder);
            }
        } catch (Exception e) {
            log.warn("Exception in decoding generic record.");
            return null;
        }
    }

    private String buildTableName() {
        return buildTableName(repository, recordType);
    }

    public static String buildTableName(String repository, String recordType) {
        return REPO + repository + RECORD + recordType;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Object convertAvroToJavaType(Object avroInstance) {
        if (avroInstance instanceof List) {
            List l = new ArrayList<>();
            for (Object o : (List) avroInstance) {
                l.add(convertAvroToJavaType(o));
            }
            return l;
        } else if (avroInstance instanceof GenericData.Record) {
            GenericData.Record r = (GenericData.Record) avroInstance;
            Map<String, Object> map = new HashMap<>();

            for (Schema.Field f : r.getSchema().getFields()) {
                map.put(f.name(), convertAvroToJavaType(r.get(f.name())));
            }

            return map;
        } else if (avroInstance instanceof Utf8) {
            return avroInstance.toString();
        } else if (avroInstance instanceof GenericData.EnumSymbol) {
            return avroInstance.toString();
        } else if (avroInstance instanceof Map) {
            Map map = (Map) avroInstance;
            Map<String, String> result = new HashMap<>();
            for (Object key : map.keySet()) {
                result.put(key.toString(), map.get(key).toString());
            }
            return result;
        }

        return avroInstance;
    }

    private Item buildItem(String id, GenericRecord record) {

        Map<String, Object> attrMap = new HashMap<>();

        Object hashKeyValue = record.get(tableIndex.getHashKeyField());
        if (hashKeyValue == null) {
            attrMap.put(ID, id);
        } else {
            attrMap.put(tableIndex.getHashKeyAttr(), hashKeyValue.toString());
            if (tableIndex.getRangeKeyAttr() != null) {
                attrMap.put(tableIndex.getRangeKeyAttr(), record.get(tableIndex.getRangeKeyField()).toString());
            }
        }

        attrMap.put(BLOB, avroToBytes(record));

        if (tableAttributes != null) {
            for (String attr : tableAttributes.getNames()) {
                attrMap.put(attr, convertAvroToJavaType(record.get(attr)));
            }
        }

        return Item.fromMap(attrMap);
    }

    private UpdateItemSpec buildUpdateItemSpec(String id, GenericRecord record) {

        DynamoKey dk = constructDynamoKey(id, record);


        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(dk.getPrimaryKey());

        updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(BLOB).put(avroToBytes(record)));

        if (tableIndex != null) {
            updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(tableIndex.getHashKeyAttr())
                    .put(record.get(tableIndex.getHashKeyField()).toString()));
            updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(tableIndex.getRangeKeyAttr())
                    .put(record.get(tableIndex.getRangeKeyField()).toString()));
        }

        return updateItemSpec;
    }

    private DynamoKey constructDynamoKey(String id, GenericRecord record) {
        DynamoKey dk = constructDynamoKey(record);
        if (dk == null ) {
            return constructDynamoKey(id);
        }
        return dk;
    }
    private DynamoKey constructDynamoKey(String id) {

        String[] ids = new String[4];
        String[] compositeIds = id.split("#");
        if (compositeIds.length > 4) {
            return null;
        } else if (compositeIds.length == 0) {
            ids[0] = id;
        } else {
            for (int i = 0; i < compositeIds.length; i++) {
                ids[i] = compositeIds[i];
            }
        }
        return constructDynamoKey(ids);
    }

    private DynamoKey constructDynamoKey(GenericRecord record) {
        try {
            String[] ids = new String[4];

            String hashKeyField = tableIndex.getHashKeyField();
            if (hashKeyField != null) {
                ids[0] = record.get(hashKeyField).toString();
            }

            String rangeKeyField = tableIndex.getRangeKeyField();
            if (rangeKeyField != null) {
                ids[1] = record.get(rangeKeyField).toString();
                String bucketField = tableIndex.getBucketKeyField();
                if (bucketField != null) {
                    ids[2] = record.get(bucketField).toString();
                    String stampField = tableIndex.getStampKeyField();
                    if (stampField != null) {
                        ids[3] = record.get(stampField).toString();
                    }
                }
            }

            return constructDynamoKey(ids);
        } catch (Exception ex) {
            return null;
        }

    }

    private DynamoKey constructDynamoKey(Map<String, String> properties) {
        String[] ids = new String[4];

        String hashKeyField = tableIndex.getHashKeyField();
        if (hashKeyField != null) {
            ids[0] = properties.get(hashKeyField);
        }

        String rangeKeyField = tableIndex.getRangeKeyField();
        if (rangeKeyField != null) {
            ids[1] = properties.get(rangeKeyField);
            String bucketField = tableIndex.getBucketKeyField();
            if (bucketField != null) {
                ids[2] = properties.get(bucketField);
                String stampField = tableIndex.getStampKeyField();
                if (stampField != null) {
                    ids[3] = properties.get(stampField);
                }
            }
        }
        return constructDynamoKey(ids);

    }

    private DynamoKey constructDynamoKey(String[] ids) {
        String hashKeyAttr = tableIndex.getHashKeyAttr();
        String rangeKeyAttr = tableIndex.getRangeKeyAttr();
        return new DynamoKey(hashKeyAttr, rangeKeyAttr, ids);
    }


   private boolean isTimeSeriesStore() {
       return (tableIndex.getBucketKeyField() != null);
   }

    private List<GenericRecord> extractRecords(Item item) {
        List<GenericRecord> records = new ArrayList<GenericRecord>();
        extractRecords(item, records);
        return records;
    }


    private void extractRecords(Item item, List<GenericRecord> records) {
        if (isTimeSeriesStore()) {
            for (Map.Entry<String,Object> entry : item.attributes()) {
                if (isBucketAttr(entry.getKey())) {
                    try {
                        Set<ByteBuffer> stampSet = item.getByteBufferSet(entry.getKey());
                        for (ByteBuffer stamp : stampSet) {
                            GenericRecord record = bytesToAvro(stamp);
                            records.add(record);
                        }
                    } catch (Exception e) {
                        log.error("Invalid time stamp records", e);
                    }
                }
             }
        } else {
             GenericRecord record = bytesToAvro(item.getByteBuffer(BLOB));
             records.add(record);
        }
    }

    private boolean isBucketAttr(String key) {
        return key.matches("[0-9]+");
    }

    @Override
    public Map<String, Object> findAttributes(String id) {
        if (isTimeSeriesStore()) {
            log.info("Find attributes for timeseries table is not supported");
        }
        DynamoKey dk = constructDynamoKey(id);
        if (dk == null) {
            return null;
        }
        return findAttributes(dk);
    }

    public Map<String, Object> findAttributes(Map<String, String> properties) {
        if (isTimeSeriesStore()) {
            log.info("Find attributes for timeseries table is not supported");
        }
        DynamoKey dk = constructDynamoKey(properties);
        if (dk == null) {
            return null;
        }

        return findAttributes(dk);
    }

    private Map<String, Object> findAttributes(DynamoKey dk) {
        DynamoDB dynamoDB = new DynamoDB(dynamoService.getClient());
        Table table = dynamoDB.getTable(tableName);
        Map<String, Object> map = new HashMap<>();

        try {
            Item item = table.getItem(dk.getPrimaryKey());
            Map<String, Object> items = item.asMap();
            Set<String> attrNames = new HashSet<>(tableAttributes.getNames());
            for (Map.Entry<String, Object> entry : items.entrySet()) {
                if (attrNames.contains(entry.getKey())) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        } catch (NoSuchMethodError e) {
            log.info("The table name is " + tableName);
            throw new RuntimeException(ERRORMESSAGE, e);
        } catch (Exception e) {
            log.error("Unable to find record " + tableName);
        }

        return map;
    }

    class DynamoKey {
        PrimaryKey pk;
        String[] ids;

        public DynamoKey(String hashAttr, String rangeAttr, String[] ids) {
            this.ids = ids;
            if (rangeAttr == null) {
                pk = new PrimaryKey(hashAttr, ids[0]);
            } else {
                pk = new PrimaryKey(hashAttr, ids[0], rangeAttr, ids[1]);
            }
        }

        public PrimaryKey getPrimaryKey() {
            return pk;
        }

        public String getHashKey() {
            return ids[0];
        }

        public String getRangeKey() {
            return ids[1];
        }

        public String getBucketKey() {
            return ids[2];
        }

        public String getStampKey() {
            return ids[3];
        }

        public String getId() {
            String id = ids[0];
            for (int i = 1; i < ids.length; i++) {
                if (ids[i] != null) {
                    id = id + "." + ids[i];
                } else {
                    break;
                }
            }
            return id;
       }
   }
}
