package com.latticeengines.datafabric.connector.generic;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;

import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class DynamoProcessorAdapter implements ProcessorAdapter {

    @Override
    public void setup(GenericSinkConnectorConfig connectorConfig) {
        // TODO Auto-generated method stub

    }

    @Override
    public int write(String repository, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs) {
        return 0;
    }

}
