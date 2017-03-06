package com.latticeengines.datafabric.connector.generic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public abstract class AbstractProcessorAdapter implements ProcessorAdapter {
    private final Log log = LogFactory.getLog(AbstractProcessorAdapter.class);

    private GenericSinkConnectorConfig connectorConfig;

    @Override
    public void setup(GenericSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    protected String getFileName(TopicPartition tp) {
        return "part-" + tp.topic() + "-" + tp.partition() + ".avro";
    }

    protected Map<String, Pair<GenericRecord, Map<String, Object>>> getPairMap(
            List<Pair<GenericRecordRequest, GenericRecord>> pairs) {
        Map<String, Pair<GenericRecord, Map<String, Object>>> pairMap = new HashMap<String, Pair<GenericRecord, Map<String, Object>>>();
        for (Pair<GenericRecordRequest, GenericRecord> pair : pairs) {
            pairMap.put(pair.getKey().getId(), Pair.of(pair.getValue(), null));
        }
        return pairMap;
    }
}
