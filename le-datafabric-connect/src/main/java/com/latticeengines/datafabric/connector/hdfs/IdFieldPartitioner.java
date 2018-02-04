package com.latticeengines.datafabric.connector.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.hdfs.partitioner.FieldPartitioner;

public class IdFieldPartitioner extends FieldPartitioner {

    private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
    private static String fieldName;
    private List<FieldSchema> partitionFields = new ArrayList<>();

    public void configure(Map<String, Object> config) {
        fieldName = (String) config.get("partition.field.name");
        partitionFields.add(new FieldSchema(fieldName, TypeInfoFactory.stringTypeInfo.toString(), ""));
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Struct key = (Struct) sinkRecord.key();
        String id = String.valueOf(key.get(fieldName));
        String customerSpace = String.valueOf(key.get("customerSpace"));
        String encodePartition = String.format("%s/%s=%s", customerSpace, fieldName, id).toString();
        log.info("encode partition: " + encodePartition);
        return encodePartition;
    }

    public List<FieldSchema> partitionFields() {
        return partitionFields;
    }

}
