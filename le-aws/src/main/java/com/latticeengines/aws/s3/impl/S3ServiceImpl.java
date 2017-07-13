package com.latticeengines.aws.s3.impl;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.latticeengines.aws.s3.S3Service;

@Component("s3Service")
public class S3ServiceImpl implements S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3ServiceImpl.class);

    @Autowired
    private AmazonS3 s3Client;

    @Override
    public void cleanupPrefix(String bucket, String prefix) {
        List<S3ObjectSummary> objects = s3Client.listObjects(bucket, prefix).getObjectSummaries();
        for (S3ObjectSummary summary : objects) {
            log.info("Deleting s3 object " + summary.getKey() + " from " + bucket);
            s3Client.deleteObject(bucket, summary.getKey());
        }
        log.info("Deleting s3 object " + prefix + " from " + bucket);
        s3Client.deleteObject(bucket, prefix);
    }

    @Override
    public List<S3ObjectSummary> listObjects(String bucket, String prefix) {
        ListObjectsRequest request = new ListObjectsRequest(bucket, prefix, null, null, Integer.MAX_VALUE);
        return s3Client.listObjects(request).getObjectSummaries();
    }

    @Override
    public MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync) {
        TransferManager tm = new TransferManager(s3Client, Executors.newFixedThreadPool(8));
        final MultipleFileUpload upload = tm.uploadDirectory(bucket, prefix, new File(localDir), true);
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

            setAclRecursive(bucket, prefix);
        }

        return upload;
    }

    @Override
    public Upload uploadInputStream(String bucket, String key, InputStream inputStream, Boolean sync) {
        TransferManager tm = new TransferManager(s3Client);
        Upload upload = tm.upload(bucket, key, inputStream, null);

        log.info(upload.getDescription());
        if (sync) {
            try {
                upload.waitForCompletion();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            setAclRecursive(bucket, key);
        }

        return upload;
    }

    @Override
    public Upload uploadLocalFile(String bucket, String key, File file, Boolean sync) {
        TransferManager tm = new TransferManager(s3Client);
        Upload upload = tm.upload(bucket, key, file);

        log.info(upload.getDescription());
        if (sync) {
            try {
                upload.waitForCompletion();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            setAclRecursive(bucket, key);
        }

        return upload;
    }

    private void setAclRecursive(String bucket, String prefix) {
        for (S3ObjectSummary summary: listObjects(bucket, prefix)) {
            s3Client.setObjectAcl(summary.getBucketName(), summary.getKey(), CannedAccessControlList.AuthenticatedRead);
        }
    }

}
