package com.latticeengines.aws.firehose.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationUpdate;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.UpdateDestinationRequest;
import com.amazonaws.services.kinesisfirehose.model.UpdateDestinationResult;
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
    public void send(String deliveryStreamName, String s3ObjectPrefix, String data) {
        try {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(deliveryStreamName);

            data = data + "\n";
            Record record = createRecord(data);
            putRecordRequest.setRecord(record);

            boolean addedPrefix = addS3ObjectPrefix(deliveryStreamName, s3ObjectPrefix);
            firehoseClient.putRecord(putRecordRequest);
            if (addedPrefix) {
                removeS3ObjectPrefix(deliveryStreamName);
            }
        } catch (Throwable t) {
            log.warn("Cannot send message to AWS Firehose delivery stream name=" + deliveryStreamName + " error="
                    + t.getClass().getName() + ": " + t.getMessage());
        }
    }

    @Override
    public void sendBatch(String deliveryStreamName, String s3ObjectPrefix, List<String> streams) {
        try {
            int batchSize = firehoseBatchSize;
            if (streams.size() > batchSize) {
                int batches = streams.size() / batchSize;
                if (streams.size() % batchSize > 0) {
                    batches++;
                }
                for (int i = 0; i < batches; i++) {
                    List<String> subStreams = streams.subList(i * batchSize, (i + 1) * batchSize);
                    sendMiniBatch(deliveryStreamName, s3ObjectPrefix, subStreams);
                }
            } else {
                sendMiniBatch(deliveryStreamName, s3ObjectPrefix, streams);
            }

        } catch (Throwable t) {
            log.warn("Cannot sendBatch message to AWS Firehose delivery stream name=" + deliveryStreamName + " error="
                    + t.getClass().getName() + ": " + t.getMessage());
        }
    }

    private void sendMiniBatch(String deliveryStreamName, String s3ObjectPrefix, List<String> streams) {
        PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(deliveryStreamName);
        List<Record> recordList = new ArrayList<>();
        for (String data : streams) {
            data = data + "\n";
            Record record = createRecord(data);
            recordList.add(record);
        }
        putRecordBatchRequest.setRecords(recordList);

        boolean addedPrefix = addS3ObjectPrefix(deliveryStreamName, s3ObjectPrefix);
        firehoseClient.putRecordBatch(putRecordBatchRequest);
        if (addedPrefix) {
            removeS3ObjectPrefix(deliveryStreamName);
        }
    }

    private Record createRecord(String data) {
        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
    }

    private boolean addS3ObjectPrefix(String deliveryStreamName, String s3ObjectPrefix) {
        if (StringUtils.isBlank(deliveryStreamName)) {
            log.warn("Firehose deliveryStreamName not provided.  Can't change prefix");
            return false;
        } else if (StringUtils.isBlank(s3ObjectPrefix)) {
            log.info("Firehose prefix for " + deliveryStreamName + " is null.  No need to change prefix");
            return false;
        }
        return setS3ObjectPrefix(deliveryStreamName, s3ObjectPrefix);
    }

    private boolean removeS3ObjectPrefix(String deliveryStreamName) {
        return setS3ObjectPrefix(deliveryStreamName, "");
    }

    private boolean setS3ObjectPrefix(String deliveryStreamName, String s3ObjectPrefix) {
        // Get DeliveryStreamDescription for delivery stream in order to extra version ID and destinations for
        // UpdateDestination call.
        DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
        describeDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        DescribeDeliveryStreamResult describeDeliveryStreamResult = firehoseClient.describeDeliveryStream(
                describeDeliveryStreamRequest);
        DeliveryStreamDescription deliveryStreamDescription = describeDeliveryStreamResult
                .getDeliveryStreamDescription();

        // Execute UpdateDestination call to set S3 Bucket folder prefix to the tenant name.
        ExtendedS3DestinationUpdate extendedS3DestinationUpdate = new ExtendedS3DestinationUpdate();
        extendedS3DestinationUpdate.setPrefix(s3ObjectPrefix);
        UpdateDestinationRequest updateDestinationRequest = new UpdateDestinationRequest();
        updateDestinationRequest.setExtendedS3DestinationUpdate(extendedS3DestinationUpdate);
        updateDestinationRequest.setDeliveryStreamName(deliveryStreamName);
        updateDestinationRequest.setCurrentDeliveryStreamVersionId(deliveryStreamDescription.getVersionId());
        log.debug("Set Firehose S3 Object Prefix UpdateDestinationRequest: " + updateDestinationRequest);
        if (CollectionUtils.isNotEmpty(deliveryStreamDescription.getDestinations())) {
            updateDestinationRequest.setDestinationId(deliveryStreamDescription.getDestinations().get(0)
                    .getDestinationId());
            UpdateDestinationResult updateDestinationResult = firehoseClient.updateDestination(
                    updateDestinationRequest);
            return true;
        } else {
            log.error("Firehose Service found null or empty destinations for delivery stream: " +
                    deliveryStreamName);
            return false;
        }
    }
}
