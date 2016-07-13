package com.latticeengines.aws.s3.impl;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.latticeengines.aws.s3.S3Service;

@Component("s3Service")
public class S3ServiceImpl implements S3Service {

    private static final Log log = LogFactory.getLog(S3ServiceImpl.class);

    @Autowired
    private AmazonS3 s3Client;

    @Value("${aws.le.stack}")
    private String awsStackName;

    @Override
    public void cleanupPrefix(String bucket, String prefix) {
        String stackSafePrefix = stackSafePrefix(prefix);
        List<S3ObjectSummary> objects = s3Client.listObjects(bucket, stackSafePrefix).getObjectSummaries();
        for (S3ObjectSummary summary : objects) {
            log.info("Deleting s3 object " + summary.getKey() + " from " + bucket);
            s3Client.deleteObject(bucket, summary.getKey());
        }
        log.info("Deleting s3 object " + stackSafePrefix + " from " + bucket);
        s3Client.deleteObject(bucket, stackSafePrefix);
    }

    @Override
    public List<S3ObjectSummary> listObjects(String bucket, String prefix) {
        String stackSafePrefix = stackSafePrefix(prefix);
        return s3Client.listObjects(bucket, stackSafePrefix).getObjectSummaries();
    }

    @Override
    public MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync) {
        String stackSafePrefix = stackSafePrefix(prefix);
        TransferManager tm = new TransferManager(s3Client, Executors.newFixedThreadPool(8));
        final MultipleFileUpload upload = tm.uploadDirectory(bucket, stackSafePrefix, new File(localDir), true);
        final AtomicInteger uploadedObjects = new AtomicInteger(0);

        ExecutorService waiters = Executors.newFixedThreadPool(8);
        final Integer numFiles = upload.getSubTransfers().size();
        log.info("Submitted an upload job for " + numFiles + " files.");
        if (sync) {
            for (final Upload subUpload : upload.getSubTransfers()) {
                waiters.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            log.info(subUpload.getDescription());
                            subUpload.waitForCompletion();
                            log.info("Uploaded " + uploadedObjects.incrementAndGet() + " out of " + numFiles + " files.");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            try {
                waiters.shutdown();
                waiters.awaitTermination(100, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return upload;
    }

    private String stackSafePrefix(String prefix) {
        if (prefix.startsWith("/")) {
            return awsStackName + prefix;
        } else {
            return awsStackName + "/" + prefix;
        }
    }

}
