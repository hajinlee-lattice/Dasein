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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.util.RedisUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.Pool;

@SuppressWarnings("deprecation")
public class RedisDataStoreImpl implements FabricDataStore {

    private static final Logger log = LoggerFactory.getLogger(RedisDataStoreImpl.class);

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
        log.info("Constructed redis data store repo " + repository + " record " + recordType + " index " + redisIndex);
    }

    @Override
    public void createRecord(String id, Pair<GenericRecord, Map<String, Object>> pair) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        createRecordInternal(pipeline, id, (pair == null) ? null : pair.getLeft());
        flushPipeline(pipeline);
        jedisPool.returnResource(jedis);
    }

    @Override
    public void updateRecord(String id, Pair<GenericRecord, Map<String, Object>> record) {
        createRecord(id, record);
    }

    @Override
    public void createRecords(Map<String, Pair<GenericRecord, Map<String, Object>>> records) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        for (Map.Entry<String, Pair<GenericRecord, Map<String, Object>>> entry : records.entrySet())
            createRecordInternal(pipeline, entry.getKey(), (entry.getValue() == null) ? null : entry.getValue().getLeft());
        flushPipeline(pipeline);
        jedisPool.returnResource(jedis);
    }

    @Override
    public Pair<GenericRecord, Map<String, Object>> findRecord(String id) {
        Jedis jedis = jedisPool.getResource();
        String key = buildKey(id);
        String jsonRecord = jedis.get(key);
        GenericRecord record = jsonToAvro(jsonRecord);
        jedisPool.returnResource(jedis);
        return (record == null) ? null : Pair.of(record, null);
    }

    @Override
    public Map<String, Pair<GenericRecord, Map<String, Object>>> batchFindRecord(List<String> idList) {
        Map<String, Pair<GenericRecord, Map<String, Object>>> records = new HashMap<>();
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        for (String id : idList) {
            pipeline.get(buildKey(id));
        }
        List<Object> results = pipeline.syncAndReturnAll();
        for (int i = 0; i < results.size(); i++) {
            String id = idList.get(i);
            GenericRecord record = jsonToAvro((String) results.get(i));
            if (record != null) {
                records.put(id, Pair.of(record, null));
            }
        }
        return records;
    }

    @Override
    public List<Pair<GenericRecord, Map<String, Object>>> findRecords(Map<String, String> properties) {
        Jedis jedis = jedisPool.getResource();

        String index = buildQuery(properties);
        Set<String> recordKeys = jedis.smembers(index);
        Pipeline pipeline = jedis.pipelined();
        for (String key : recordKeys) {
            pipeline.get(key);
        }
        List<Object> results = pipeline.syncAndReturnAll();

        List<Pair<GenericRecord, Map<String, Object>>> records = new ArrayList<>();
        for (Object obj : results) {
            GenericRecord record = jsonToAvro((String) obj);
            if (record != null) {
                records.add(Pair.of(record, null));
            }
        }
        jedisPool.returnResource(jedis);

        return records;
    }

    @Override
    public void deleteRecord(String id, GenericRecord record) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        deleteRecordInternal(pipeline, id, record);
        jedisPool.returnResource(jedis);
    }

    private void createRecordInternal(Pipeline pipeline, String id, GenericRecord record) {
        String key = buildKey(id);
        pipeline.set(key, avroToJson(record));
        if (indexes == null)
            return;
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
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output);
            writer.write(record, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } catch (Exception e) {
            return null;
        }
    }

    private GenericRecord jsonToAvro(String json) {
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            try (InputStream input = new ByteArrayInputStream(json.getBytes())) {
                DataInputStream din = new DataInputStream(input);
                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
                GenericRecord datum = reader.read(null, decoder);
                return datum;
            }
        } catch (Exception e) {
            return null;
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

    @Override
    public Map<String, Object> findAttributes(String id) {
        throw new UnsupportedOperationException();
    }

}
