package com.latticeengines.datafabric.connector.redis;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.FabricDataServiceImpl;
import com.latticeengines.datafabric.service.datastore.impl.RedisDataServiceProvider;

import io.confluent.connect.avro.AvroData;

public class RedisSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
    private AvroData avroData;
    private FabricDataService dataService;
    private FabricDataStore dataStore;
    private String repository;
    private String recordType;

    public RedisSinkTask() {

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            RedisSinkConnectorConfig connectorConfig = new RedisSinkConnectorConfig(props);
            int schemaCacheSize = connectorConfig.getInt(RedisSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
            avroData = new AvroData(schemaCacheSize);
            String servers = connectorConfig.getString(RedisSinkConnectorConfig.REDIS_SERVERS_CONFIG);
            int port = connectorConfig.getInt(RedisSinkConnectorConfig.REDIS_PORT_CONFIG);
            boolean haEnabled = connectorConfig.getBoolean(RedisSinkConnectorConfig.REDIS_HA_CONFIG);
            String master = connectorConfig.getString(RedisSinkConnectorConfig.REDIS_MASTER_CONFIG);
            repository = connectorConfig.getString(RedisSinkConnectorConfig.REDIS_REPO_CONFIG);
            recordType = connectorConfig.getString(RedisSinkConnectorConfig.REDIS_RECORD_CONFIG);

            dataService = new FabricDataServiceImpl();
            RedisDataServiceProvider redis = new RedisDataServiceProvider(master, servers, port, 4, haEnabled);
            dataService.addServiceProvider(redis);
            dataStore = null;

            // No recovery implemented assuming sinking to Redis is idempotent.
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start RedisSinkConnector due to configuration error.", e);
        } catch (ConnectException e) {
            log.info("Couldn't start RedisSinkConnector:", e);
            log.info("Shutting down ReidsSinkConnector.");
        }
    }

    @Override
    public void stop() throws ConnectException {
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {

        Map<String, GenericRecord> avroRecords;

        avroRecords = new HashMap<String, GenericRecord>();

        try {
            for (SinkRecord record: records) {
                Struct value = (Struct)record.value();
                Struct key = (Struct)record.key();
                if ((value == null) || (key == null)) continue;
                GenericRecord valueRec = (GenericRecord)avroData.fromConnectData(value.schema(), value);
                GenericRecord keyRec = (GenericRecord)avroData.fromConnectData(key.schema(), key);
                String id = keyRec.get("id").toString();;
                String recordType = keyRec.get("record").toString();;
                log.debug("Redis Connector Got record " + recordType + " id " + id + "\n");
                if (!recordType.equals(this.recordType))
                    continue;
                if (dataStore == null) {
                    dataStore = dataService.constructDataStore("REDIS", repository, recordType, valueRec.getSchema());
                }
                avroRecords.put(id, valueRec);
            }
            if (dataStore != null) {
                dataStore.createRecords(avroRecords);
            }
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

    public AvroData getAvroData() {
        return avroData;
    }
}
