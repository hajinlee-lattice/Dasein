package com.latticeengines.datafabric.service.datastore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.util.RedisUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.Pool;

public class RedisDataStoreImpl implements FabricDataStore {

    private static final Log log = LogFactory.getLog(RedisDataStoreImpl.class);

    private Pool<Jedis> jedisPool;

    private String repository;
    private String recordType;
    private Schema schema;

    private final String REPO = "_REPO_";
    private final String RECORD = "_RECORD_";
    private final String KEY = "_KEY_";
    private final String INDEX = "_INDEX_";
    private final String INDEXKEY = "_INDEXKEY";
    private final String FIELD = "_FIELD_";
    private final String VALUE = "_VALUE_";

    private Map<String, List<String>> indexes = null;

    public RedisDataStoreImpl(Pool<Jedis> jedisPool, String repository, String recordType, Schema schema) {

        this.jedisPool = jedisPool;
        this.repository = repository;
        this.recordType = recordType;
        this.schema = schema;
        this.indexes = null;

        Jedis jedis = jedisPool.getResource();
        String redisIndex = schema.getProp(RedisUtil.INDEX);
        if (redisIndex == null) {
            redisIndex = jedis.get(buildIndexKey());
            if (redisIndex != null)
                schema.addProp(RedisUtil.INDEX, redisIndex);
        } else {
            jedis.set(buildIndexKey(), redisIndex);
        }

        jedisPool.returnResource(jedis);

        if (redisIndex != null) {
            this.indexes = RedisUtil.getIndex(redisIndex);
        }
        log.info("Constructed redis data store repo " + repository + " record " + recordType +
                 " index " + redisIndex);
    }

    public void createRecord(String id, GenericRecord record) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        createRecordInternal(pipeline, id, record);
        flushPipeline(pipeline);
        jedisPool.returnResource(jedis);
    }

    public void updateRecord(String id, GenericRecord record) {
        createRecord(id, record);
    }

    public void createRecords(Map<String, GenericRecord> records) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        for (Map.Entry<String, GenericRecord> entry : records.entrySet())
            createRecordInternal(pipeline, entry.getKey(), entry.getValue());
        flushPipeline(pipeline);
        jedisPool.returnResource(jedis);
    }

    public GenericRecord findRecord(String id) {
        Jedis jedis = jedisPool.getResource();
        String key = buildKey(id);
        String jsonRecord = jedis.get(key);
        GenericRecord record = jsonToAvro(jsonRecord);
        jedisPool.returnResource(jedis);
        return record;
    }

    public List<GenericRecord> findRecords(Map<String, String> properties)  {
        Jedis jedis = jedisPool.getResource();

        String index = buildQuery(properties);
        Set<String> recordKeys = jedis.smembers(index);
        Pipeline pipeline = jedis.pipelined();
        for (String key : recordKeys) {
             pipeline.get(key);
        }
        List<Object> results = pipeline.syncAndReturnAll();

        List<GenericRecord> records = new ArrayList<GenericRecord>();
        for (Object obj : results) {
             GenericRecord record = jsonToAvro((String)obj);
             if (record != null)
                 records.add(jsonToAvro((String)obj));
        }
        jedisPool.returnResource(jedis);

        return records;
    }

    public void deleteRecord(String id, GenericRecord record)  {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        deleteRecordInternal(pipeline, id, record);
        jedisPool.returnResource(jedis);
    }

    private void createRecordInternal(Pipeline pipeline, String id, GenericRecord record) {
        String key = buildKey(id);
        pipeline.set(key, avroToJson(record));
        if (indexes == null) return;
        for (List<String> index : indexes.values()) {
            String indexKey = buildIndex(record, index);
            pipeline.sadd(indexKey, key);
        }
    }

    private void deleteRecordInternal(Pipeline pipeline, String id, GenericRecord record) {
        String key = buildKey(id);
        if (indexes != null) {
            for (List<String> index : indexes.values()) {
                String indexKey = buildIndex(record, index);
                pipeline.srem(indexKey, key);
            }
        }
        pipeline.del(key);
    }

    private List<Object> flushPipeline(Pipeline pipeline) {

        List<Object> results = pipeline.syncAndReturnAll();
       return results;
    }

    private String avroToJson(GenericRecord record) {
        boolean pretty = false;
        GenericDatumReader<GenericRecord> reader = null;
        JsonEncoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            output = new ByteArrayOutputStream();
            reader = new GenericDatumReader<GenericRecord>();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            writer.write(record, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } catch (Exception e) {
            return null;
        } finally {
            try { if (output != null) output.close(); } catch (Exception e) { }
        }
    }

    private GenericRecord jsonToAvro(String json) {
        InputStream input = null;
        GenericRecord datum = null;
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            input = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(input);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            datum = reader.read(null, decoder);
            return datum;
        } catch (Exception e) {
            return null;
        } finally {
            try { input.close(); } catch (Exception e) { }
        }
    }

    private String buildKey(String id) {
        return REPO + repository + RECORD + recordType + KEY + id;
    }

    private String buildIndexKey() {
        return REPO + repository + RECORD + recordType + INDEXKEY;
    }

    private String buildIndex(GenericRecord record, List<String> index) {
        StringBuilder builder = new StringBuilder();
        builder.append(REPO + repository + RECORD + recordType + INDEX);
        Map<String, String> properties = new HashMap<String, String>();
        for (String field : index) {
            String value = record.get(field).toString();
            builder.append(FIELD + field + VALUE + value);
        }
        return builder.toString();
    }

    private String buildQuery(Map<String, String> properties) {
        StringBuilder builder = new StringBuilder();
        builder.append(REPO + repository + RECORD + recordType + INDEX);
        List<String> indexes = new ArrayList<String>(properties.keySet());
        Collections.sort(indexes);
        for (String field : indexes) {
            String value = properties.get(field);
            builder.append(FIELD + field + VALUE + value);
        }
        return builder.toString();
    }

}
