package com.latticeengines.datafabric.connector.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import com.latticeengines.common.exposed.util.AvroUtils;

import io.confluent.connect.avro.AvroData;

class AvroTopicPartitionBuffer {

    private static final Logger log = LoggerFactory.getLogger(AvroTopicPartitionBuffer.class);

    private final TopicPartition tp;
    private Schema schema;
    private final File avroFile;
    private Integer rows = 0;
    private Long finalOffset = -1L;
    private Set<Long> offsets = new HashSet<>();

    AvroTopicPartitionBuffer(TopicPartition tp) {
        log.info("Initialize buffer for TopicPartition " + tp);
        this.tp = tp;
        this.avroFile = new File(String.format("%s/%s/%d.avro", S3SinkConstants.TMP_ROOT, tp.topic(), tp.partition()));
        try {
            FileUtils.forceMkdir(new File(String.format("%s/%s", S3SinkConstants.TMP_ROOT, tp.topic())));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create topic folder for " + tp, e);
        }
        reset();
    }

    void setSchema(Schema schema) {
        if (this.schema == null) {
            this.schema = schema;
        }
    }

    TopicPartition getTopicPartition() {
        return new TopicPartition(tp.topic(), tp.partition());
    }

    File getAvroFile() {
        return avroFile;
    }

    void write(List<SinkRecord> records, AvroData avroData) {
        List<GenericRecord> genericRecords = new ArrayList<>();
        Schema schema = null;
        Long lastOffset = -1L;
        for (SinkRecord record : records) {
            schema = avroData.fromConnectSchema(record.valueSchema());
            Long thisOffset = record.kafkaOffset();
            if (!offsets.contains(thisOffset)) {
                GenericRecord value = (GenericRecord) //
                avroData.fromConnectData(record.valueSchema(), record.value());
                genericRecords.add(value);
                lastOffset = Math.max(lastOffset, thisOffset);
                offsets.add(thisOffset);
            }
        }

        setSchema(schema);

        try {
            writeDataToFile(genericRecords);
        } catch (IOException e) {
            log.error("Failed to write data to local file " + avroFile.getAbsolutePath(), e);
        }
        this.finalOffset = lastOffset;
        log.debug("Update buffer  " + tp + " finalOffset to " + this.finalOffset);
    }

    void writeDataToFile(List<GenericRecord> data) throws IOException {
        if (avroFile.exists()) {
            AvroUtils.appendToLocalFile(data, avroFile.getAbsolutePath());
        } else {
            AvroUtils.writeToLocalFile(schema, data, avroFile.getAbsolutePath());
        }
        this.rows += data.size();
        log.debug(String.format("Write %d generic records to local file.", data.size()));
    }

    Long getFinalOffset() {
        return finalOffset;
    }

    Integer getRows() {
        return rows;
    }

    void reset() {
        FileUtils.deleteQuietly(avroFile);
        rows = 0;
        offsets = new HashSet<>();
    }
}
