package com.latticeengines.aws.firehose.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.latticeengines.aws.firehose.FirehoseService;

@Component("firehoseService")
public class FirehoseServiceImpl implements FirehoseService {

    private static final Logger log = LoggerFactory.getLogger(FirehoseServiceImpl.class);

    private AmazonKinesisFirehose firehoseClient;

    @Value("${aws.etl.firehose.batch.size:200}")
    private int firehoseBatchSize;

    @Autowired
    public FirehoseServiceImpl(BasicAWSCredentials etlCredentials, @Value("${aws.region}") String region) {
        log.info("Constructing AWS Firehose client using BasicAWSCredentials.");
        firehoseClient = AmazonKinesisFirehoseClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(etlCredentials)) //
                .withRegion(region) //
                .build();
    }

    @Override
    public void send(String deliveryStreamName, String data) {
        try {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(deliveryStreamName);

            data = data + "\n";
            Record record = createRecord(data);
            putRecordRequest.setRecord(record);
            firehoseClient.putRecord(putRecordRequest);
        } catch (Throwable t) {
            log.warn("Cannot send message to AWS Firehose delivery stream name=" + deliveryStreamName + " error="
                    + t.getClass().getName() + ": " + t.getMessage());
        }
    }

    @Override
    public void sendBatch(String deliveryStreamName, List<String> streams) {
        try {
            int batchSize = firehoseBatchSize;
            if (streams.size() > batchSize) {
                int batches = streams.size() / batchSize;
                if (streams.size() % batchSize > 0) {
                    batches++;
                }
                for (int i = 0; i < batches; i++) {
                    // Handle case where final mini-batch is not the full size.
                    int endOfRange = Math.min((i + 1) * batchSize, streams.size());
                    List<String> subStreams = streams.subList(i * batchSize, endOfRange);
                    sendMiniBatch(deliveryStreamName, subStreams);
                }
            } else {
                sendMiniBatch(deliveryStreamName, streams);
            }

        } catch (Throwable t) {
            log.warn("Cannot sendBatch message to AWS Firehose delivery stream name=" + deliveryStreamName + " error="
                    + t.getClass().getName() + ": " + t.getMessage(), t);
        }
    }

    private void sendMiniBatch(String deliveryStreamName, List<String> streams) {
        PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(deliveryStreamName);
        List<Record> recordList = new ArrayList<>();
        for (String data : streams) {
            data = data + "\n";
            Record record = createRecord(data);
            recordList.add(record);
        }
        putRecordBatchRequest.setRecords(recordList);
        firehoseClient.putRecordBatch(putRecordBatchRequest);
    }

    private Record createRecord(String data) {
        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
    }
}
