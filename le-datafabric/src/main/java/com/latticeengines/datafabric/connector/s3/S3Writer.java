package com.latticeengines.datafabric.connector.s3;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

class S3Writer {

    private static final Log log = LogFactory.getLog(S3Writer.class);
    private final String bucket;
    private final String prefix;
    private final AmazonS3 client;
    private final TransferManager tm;

    S3Writer(S3SinkConfig config) {
        bucket = config.getProperty(S3SinkConfig.S3_BUCKET, String.class);
        String s3Prefix = config.getProperty(S3SinkConfig.S3_PREFIX, String.class);

        String accessKeyId = config.getProperty(S3SinkConfig.AWS_ACCESS_KEY_ID, String.class);
        String secretKey = config.getProperty(S3SinkConfig.AWS_SECRET_KEY, String.class);
        AmazonS3 s3Client;
        if (StringUtils.isNotEmpty(accessKeyId) && StringUtils.isNotEmpty(secretKey)) {
            AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretKey);
            s3Client = new AmazonS3Client(credentials);
        } else {
            // Use default credentials provider that looks in Env + Java
            // properties + profile + instance role
            s3Client = new AmazonS3Client();
        }
        if (s3Prefix.endsWith("/")) {
            s3Prefix = s3Prefix.substring(0, s3Prefix.lastIndexOf("/"));
        }
        this.prefix = s3Prefix;
        this.client = s3Client;
        this.tm = new TransferManager(s3Client);
    }

    void initialize() {
        log.info("Initializing prefix " + prefix + " in bucket " + bucket);
        List<S3ObjectSummary> objects = client.listObjects(bucket, prefix).getObjectSummaries();
        for (S3ObjectSummary summary : objects) {
            String key = summary.getKey();
            try {
                log.info("Removing an existing object " + key);
                client.deleteObject(bucket, key);
            } catch (Exception e) {
                log.error("Failed to delete object " + key + " from bucket " + bucket, e);
            }
        }
    }

    long commit(AvroTopicPartitionBuffer buffer) {
        TopicPartition tp = buffer.getTopicPartition();
        String key = fullKey(tp, buffer.getFinalOffset());
        try {
            log.info("Committing local buffer of partition " + tp + " to S3 bucket=" + bucket + " : key=" + key);
            Upload upload = tm.upload(this.bucket, key, buffer.getAvroFile());
            upload.waitForCompletion();
            client.setObjectAcl(bucket, key, CannedAccessControlList.AuthenticatedRead);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to upload avro to S3 for " + tp);
        }
        return buffer.getFinalOffset();
    }

    long getLastCommittedOffset(TopicPartition tp) {
        Long offset = -1L;
        List<S3ObjectSummary> objects = client.listObjects(bucket, prefix).getObjectSummaries();
        for (S3ObjectSummary summary : objects) {
            String key = summary.getKey();
            String filename = StringUtils.substringAfterLast(key, "/");
            if (!filename.startsWith(tp.topic()) || !filename.endsWith(".avro")) {
                // not an avro file for this topic
                continue;
            }
            String[] tokens = filename.replace(tp.topic() + "-", "").replace(".avro", "").split("-");
            Integer partition = Integer.valueOf(tokens[0]);
            if (partition == tp.partition()) {
                offset = Math.max(Long.valueOf(tokens[1]), offset);
            }
        }
        return offset;
    }

    private String fullKey(TopicPartition tp, long offset) {
        return String.format("%s/%s-%06d-%012d.avro", prefix, tp.topic(), tp.partition(), offset);
    }

}
