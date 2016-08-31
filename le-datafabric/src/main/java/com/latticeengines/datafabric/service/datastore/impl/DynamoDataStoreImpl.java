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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

        log.info("Constructed Dynamo data store repo " + repository + " record " + recordType +
                 " attributes " + attributeProp);
    }


    public void createRecord(String id, GenericRecord record) {

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        Item item = buildItem(id, record);
        try {
            table.putItem(item);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to save record " + tableName + " id " + id, e);
        }
    }

    public void deleteRecord(String id, GenericRecord record)  {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);

        try {
            table.deleteItem(ID, id);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
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
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to update record " + tableName + " id " + id, e);
        }
    }

    public GenericRecord findRecord(String id) {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        GenericRecord record = null;
        try {
            Item item = table.getItem(ID, id);
            if (item != null) {
                ByteBuffer blob = item.getByteBuffer(BLOB);
                record = jsonToAvro(blob);
            }
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to find record " + tableName + " id " + id, e);
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

        try {
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(writeItems);
            do {

                    // Check for unprocessed keys which could happen if you exceed provisioned throughput

                    Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                    if (outcome.getUnprocessedItems().size() != 0) {
                        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                    }

             } while (outcome.getUnprocessedItems().size() > 0);
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to batch create records " + tableName, e);
        }
    }


    public Map<String, GenericRecord> batchFindRecord(List<String> idList) {

        Map<String, GenericRecord> records = new HashMap<String, GenericRecord>();

        DynamoDB dynamoDB = new DynamoDB(client);
        TableKeysAndAttributes keys = new TableKeysAndAttributes(tableName);

        for (String id : idList) {
            if (id == null) continue;
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
                     GenericRecord record = jsonToAvro(item.getByteBuffer(BLOB));
                     records.put(item.getString(ID), record);
                }

                // Check for unprocessed keys which could happen if you exceed provisioned
                // throughput or reach the limit on response size.
                unprocessed = outcome.getUnprocessedKeys();

                if (!unprocessed.isEmpty()) {
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessed);
                }
            } while (!unprocessed.isEmpty());
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to batch get records " + tableName, e);
        }
        return records;
    }

    public List<GenericRecord> findRecords(Map<String, String> properties)  {

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        QuerySpec querySpec = buildQuerySpec(properties);

        if (querySpec == null) return null;

        List<GenericRecord> records = new ArrayList<GenericRecord>();
        try {
            ItemCollection<QueryOutcome> items = table.query(querySpec);

            Iterator<Item> iterator = items.iterator();
            while (iterator.hasNext()) {
                Item item = iterator.next();
                GenericRecord record = jsonToAvro(item.getByteBuffer(BLOB));
                records.add(record);
            }
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("If you see NoSuchMethodError on jackson json, " +
                    "it might because the table name or key attributes are wrong.");
        } catch (Exception e) {
            log.error("Unable to find records " + tableName, e);
        }

        return records;
    }

    private ByteBuffer avroToJson(GenericRecord record) {
        Schema schema = record.getSchema();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output);
                writer.write(record, encoder);
                encoder.flush();
                output.flush();
                return ByteBuffer.wrap(output.toByteArray());
        } catch (Exception e) {
            return null;
        }
    }

    private GenericRecord jsonToAvro(ByteBuffer json) {
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

            byte[] bytes = new byte[json.remaining()];
            json.get(bytes, 0, bytes.length);
            try (InputStream input = new ByteArrayInputStream(bytes)) {
                 DataInputStream din = new DataInputStream(input);
                 Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
                 GenericRecord datum = reader.read(null, decoder);
                 return datum;
            }
        } catch (Exception e) {
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
        attrMap.put(BLOB, avroToJson(record));

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

        updateItemSpec = updateItemSpec.addAttributeUpdate(new AttributeUpdate(BLOB).put(avroToJson(record)));

        if (tableIndex !=null) {
              updateItemSpec = updateItemSpec.addAttributeUpdate(
                  new AttributeUpdate(tableIndex.getHashKeyAttr()).put(record.get(tableIndex.getHashKeyField()).toString()));
              updateItemSpec = updateItemSpec.addAttributeUpdate(
                  new AttributeUpdate(tableIndex.getRangeKeyAttr()).put(record.get(tableIndex.getRangeKeyField()).toString()));
        }

        return updateItemSpec;
    }

    private QuerySpec buildQuerySpec(Map<String, String> properties) {
        String hashValue = properties.get(tableIndex.getHashKeyField());
        if (hashValue == null) return null;
        String rangeValue = properties.get(tableIndex.getRangeKeyField());

        StringBuilder builder = new StringBuilder();
        builder.append(tableIndex.getHashKeyAttr() + " = " + hashValue);
        if (rangeValue != null) {
             builder.append(" and " + tableIndex.getRangeKeyAttr() + " = " + rangeValue);
        }
        return new QuerySpec().withKeyConditionExpression(builder.toString());
    }

}
