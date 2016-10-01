package com.latticeengines.datafabric.service.datastore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xerial.snappy.Snappy;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;

public class DynamoDataStoreImpl implements FabricDataStore {

    private static final Log log = LogFactory.getLog(DynamoDataStoreImpl.class);

    private static final String REPO = "_REPO_";
    private static final String RECORD = "_RECORD_";
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(30);

    // Default attributes for every table
    public final String ID = "Id";
    private final String BLOB = "Record";

    private String repository;
    private String recordType;
    private Schema schema;
    private AmazonDynamoDBClient client;

    private DynamoIndex tableIndex;
    private String tableName = null;

    public DynamoDataStoreImpl(AmazonDynamoDBClient client, String repository, String recordType, Schema schema) {

        this.client = client;
        this.repository = repository;
        this.recordType = recordType;
        this.tableName = buildTableName();
        this.schema = schema;

        String attributeProp = schema.getProp(DynamoUtil.ATTRIBUTES);
        this.tableIndex = DynamoUtil.getAttributes(attributeProp);

        log.info("Constructed Dynamo data store repo " + repository + " record " + recordType + " attributes "
                + attributeProp);
    }

    public void createRecord(String id, GenericRecord record) {

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        Item item = buildItem(id, record);
        try {
            table.putItem(item);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to save record " + tableName + " id " + id, e);
        }
    }

    public void deleteRecord(String id, GenericRecord record) {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);

        try {
            table.deleteItem(ID, id);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to delete record " + tableName + " id " + id, e);
        }
    }

    public void updateRecord(String id, GenericRecord record) {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);

        UpdateItemSpec updateItemSpec = buildUpdateItemSpec(id, record);
        try {
            table.updateItem(updateItemSpec);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to update record " + tableName + " id " + id, e);
        }
    }

    public GenericRecord findRecord(String id) {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        GenericRecord record = null;
        Item item = null;
        try {
            item = table.getItem(ID, id);
        } catch (NoSuchMethodError e) {
            log.info("The table name is " + tableName);
            log.info("The key is " + id);
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to find record " + tableName + " id " + id, e);
        }
        if (item != null) {
            ByteBuffer blob = item.getByteBuffer(BLOB);
            record = bytesToAvro(blob);
        }
        return record;
    }

    public void createRecords(Map<String, GenericRecord> records) {
        DynamoDB dynamoDB = new DynamoDB(client);
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
                log.warn("If you see NoSuchMethodError on jackson json, "
                        + "it might because 0 items is prcessed, due to exceeding provisioned capacity"
                        + ", or the table name or key attributes are wrong.", e);
                try {
                    Thread.sleep(interval);
                    interval *= 2;
                    retries++;
                } catch (Exception e1) {
                    log.warn("Failed to sleep. Ignoring the error.", e1);
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

    public Map<String, GenericRecord> batchFindRecord(List<String> idList) {

        Map<String, GenericRecord> records = new HashMap<String, GenericRecord>();

        DynamoDB dynamoDB = new DynamoDB(client);
        TableKeysAndAttributes keys = new TableKeysAndAttributes(tableName);

        for (String id : idList) {
            if (id == null)
                continue;
            keys = keys.addPrimaryKey(new PrimaryKey(ID, id));
        }

        List<PrimaryKey> pKs = keys.getPrimaryKeys();

        if ((pKs == null) || (pKs.size() == 0)) {
            return records;
        }

        try {

            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(keys);
            Map<String, KeysAndAttributes> unprocessed = null;

            do {
                List<Item> items = outcome.getTableItems().get(tableName);
                for (Item item : items) {
                    GenericRecord record = bytesToAvro(item.getByteBuffer(BLOB));
                    records.put(item.getString(ID), record);
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
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to batch get records " + tableName, e);
        }
        return records;
    }

    public List<GenericRecord> findRecords(Map<String, String> properties) {

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        QuerySpec querySpec = buildQuerySpec(properties);

        if (querySpec == null)
            return null;

        List<GenericRecord> records = new ArrayList<GenericRecord>();
        try {
            ItemCollection<QueryOutcome> items = table.query(querySpec);

            Iterator<Item> iterator = items.iterator();
            while (iterator.hasNext()) {
                Item item = iterator.next();
                GenericRecord record = bytesToAvro(item.getByteBuffer(BLOB));
                records.add(record);
            }
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, "
                    + "it might because the table name or key attributes are wrong.", e);
        } catch (Exception e) {
            log.error("Unable to find records " + tableName, e);
        }

        return records;
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
            log.warn("Exception in encoding generic record.", e);
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
            log.warn("Exception in decoding generic record.", e);
            return null;
        }
    }

    private String buildTableName() {
        return buildTableName(repository, recordType);
    }

    public static String buildTableName(String repository, String recordType) {
        return REPO + repository + RECORD + recordType;
    }

    private Item buildItem(String id, GenericRecord record) {
        Map<String, Object> attrMap = new HashMap<>();

        attrMap.put(ID, id);
        attrMap.put(BLOB, avroToBytes(record));

        if (tableIndex != null) {
            attrMap.put(tableIndex.getHashKeyAttr(), record.get(tableIndex.getHashKeyField()).toString());
            if (tableIndex.getRangeKeyAttr() != null) {
                attrMap.put(tableIndex.getRangeKeyAttr(), record.get(tableIndex.getRangeKeyField()).toString());
            }
        }
        return Item.fromMap(attrMap);
    }

    private UpdateItemSpec buildUpdateItemSpec(String id, GenericRecord record) {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(ID, id);

        updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(BLOB).put(avroToBytes(record)));

        if (tableIndex != null) {
            updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(tableIndex.getHashKeyAttr())
                    .put(record.get(tableIndex.getHashKeyField()).toString()));
            updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(tableIndex.getRangeKeyAttr())
                    .put(record.get(tableIndex.getRangeKeyField()).toString()));
        }

        return updateItemSpec;
    }

    private QuerySpec buildQuerySpec(Map<String, String> properties) {
        String hashValue = properties.get(tableIndex.getHashKeyField());
        if (hashValue == null)
            return null;
        String rangeValue = properties.get(tableIndex.getRangeKeyField());

        StringBuilder builder = new StringBuilder();
        builder.append(tableIndex.getHashKeyAttr() + " = " + hashValue);
        if (rangeValue != null) {
            builder.append(" and " + tableIndex.getRangeKeyAttr() + " = " + rangeValue);
        }
        return new QuerySpec().withKeyConditionExpression(builder.toString());
    }

}
