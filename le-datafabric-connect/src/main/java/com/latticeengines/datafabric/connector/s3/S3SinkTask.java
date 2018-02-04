package com.latticeengines.datafabric.connector.s3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.confluent.connect.avro.AvroData;

public class S3SinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

    private AvroData avroData;
    private S3Writer s3;
    private Map<TopicPartition, AvroTopicPartitionBuffer> buffers = new HashMap<>();

    public S3SinkTask() {
    }

    @Override
    public String version() {
        return S3SinkConstants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            S3SinkConfig config = new S3SinkConfig(props);
            avroData = config.constructAvroData();
            s3 = new S3Writer(config);
            // Recover initial assignments
            openBuffers(context.assignment());
        } catch (ConfigException e) {
            throw new ConnectException(
                    String.format("Couldn't start %s due to configuration error.", getClass().getSimpleName()), e);
        }
    }

    @Override
    public void stop() throws ConnectException {
        for (TopicPartition tp : buffers.keySet()) {
            buffers.get(tp).reset();
        }
        buffers = new HashMap<>();
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        Map<TopicPartition, List<SinkRecord>> partitionedRecords = new HashMap<>();
        Set<TopicPartition> missingTps = new HashSet<>();
        for (SinkRecord record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            if (!buffers.containsKey(tp)) {
                missingTps.add(tp);
            }
            if (!partitionedRecords.containsKey(tp)) {
                partitionedRecords.put(tp, new ArrayList<SinkRecord>());
            }
            partitionedRecords.get(tp).add(record);
        }
        for (TopicPartition tp : partitionedRecords.keySet()) {
            AvroTopicPartitionBuffer buffer = buffers.get(tp);
            buffer.write(partitionedRecords.get(tp), avroData);
        }
        if (!missingTps.isEmpty()) {
            log.error("Cannot find these topic partitions in assignment: " + new ArrayList<>(missingTps));
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitBuffers();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) throws ConnectException {
        openBuffers(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            AvroTopicPartitionBuffer buffer = buffers.get(tp);
            if (buffer != null) {
                buffer.reset();
            }
            buffers.remove(tp);
        }
    }

    public AvroData getAvroData() {
        return avroData;
    }

    private void commitBuffer(TopicPartition tp) {
        context.pause(tp);
        try {
            AvroTopicPartitionBuffer buffer = buffers.get(tp);
            if (buffer != null && buffer.getRows() > 0) {
                long committedOffset = s3.commit(buffer);
                context.offset(tp, committedOffset + 1);
                buffers.put(tp, new AvroTopicPartitionBuffer(tp));
            } else {
                log.debug("Noting to commit for " + tp);
            }
        } catch (Exception e) {
            log.error("Failed to commit " + tp, e);
        } finally {
            context.resume(tp);
        }
    }

    private void commitBuffers() {
        Set<TopicPartition> assignment = context.assignment();
        for (TopicPartition tp : assignment) {
            commitBuffer(tp);
        }
    }

    private void openBuffers(Collection<TopicPartition> partitions) throws ConnectException {
        for (TopicPartition tp : partitions) {
            // See if this is a new assignment
            if (!buffers.containsKey(tp)) {
                log.info(String.format("Assigned new partition %s creating buffer s3", tp.toString()));
                try {
                    recoverOffsets(tp);
                } catch (Exception e) {
                    throw new ConnectException("Failed to resume TopicPartition from S3", e);
                }
            }
        }
    }

    private void recoverOffsets(TopicPartition tp) {
        this.context.pause(tp);
        long nextOffset = s3.getLastCommittedOffset(tp) + 1;
        log.info(String.format("Recovering partition %s to offset %d", tp.toString(), nextOffset));
        buffers.put(tp, new AvroTopicPartitionBuffer(tp));
        this.context.offset(tp, nextOffset);
        this.context.resume(tp);
    }
}
