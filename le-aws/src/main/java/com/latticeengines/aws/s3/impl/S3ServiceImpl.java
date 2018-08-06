package com.latticeengines.aws.s3.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.latticeengines.aws.s3.S3Service;

@Component("s3Service")
public class S3ServiceImpl implements S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3ServiceImpl.class);

    @Inject
    private AmazonS3 s3Client;

    @Override
    public boolean objectExist(String bucket, String object) {
        return s3Client.doesObjectExist(bucket, object);
    }

    @Override
    public boolean isNonEmptyDirectory(String bucket, String prefix) {
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result = s3Client.listObjectsV2(request);
        return result.getKeyCount() > 0;
    }

    @Override
    public void cleanupPrefix(String bucket, String prefix) {
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket, prefix).getObjectSummaries();
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

    @Override
    public void downloadS3File(S3ObjectSummary itemDesc, File file) throws Exception
    {
        byte[] buf = new byte[16384];
        try (S3ObjectInputStream stream = s3Client.getObject(itemDesc.getBucketName(), itemDesc.getKey()).getObjectContent()) {
            try (FileOutputStream writer = new FileOutputStream(file)) {
                while (stream.available() > 0) {
                    int bytes = stream.read(buf);
                    if (bytes <= 0)
                        break;

                    writer.write(buf, 0, bytes);
                }
            }
        }
    }

    @Override
    public InputStream readObjectAsStream(String bucket, String object) {
        if (s3Client.doesObjectExist(bucket, object)) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, object);
            try {
                S3Object s3Object = s3Client.getObject(getObjectRequest);
                log.info(String.format("Reading the object %s of type %s and size %s", object,
                        s3Object.getObjectMetadata().getContentType(),
                        FileUtils.byteCountToDisplaySize(s3Object.getObjectMetadata().getContentLength())));
                return s3Object.getObjectContent();
            } catch (AmazonS3Exception e) {
                throw new RuntimeException("Failed to get object " + object + " from S3 bucket " + bucket, e);
            }
        } else {
            log.info("Object " + object + " does not exist in bucket " + bucket);
            return null;
        }
    }
}
