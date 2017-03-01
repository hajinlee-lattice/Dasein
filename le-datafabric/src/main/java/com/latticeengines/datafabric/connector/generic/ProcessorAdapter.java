package com.latticeengines.datafabric.connector.generic;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;

import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public interface ProcessorAdapter {

    void setup(GenericSinkConnectorConfig connectorConfig);

    int write(String repository, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs);

}
