package com.latticeengines.aws.s3.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@Component("s3Service")
public class S3ServiceImpl implements S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3ServiceImpl.class);
    private static final CannedAccessControlList ACL = CannedAccessControlList.BucketOwnerFullControl;

    private static ExecutorService workers;

    @Inject
    private AmazonS3 s3Client;

    @Override
    public boolean objectExist(String bucket, String object) {
        return s3Client.doesObjectExist(bucket, object);
    }

    @Override
    public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
            String destinationKey) {
        sourceKey = sanitizePathToKey(sourceKey);
        destinationKey = sanitizePathToKey(destinationKey);
        s3Client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public void moveObject(String sourceBucketName, String sourceKey, String destinationBucketName,
            String destinationKey) {
        sourceKey = sanitizePathToKey(sourceKey);
        destinationKey = sanitizePathToKey(destinationKey);
        s3Client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        s3Client.deleteObject(sourceBucketName, sourceKey);
    }

    @Override
    public boolean isNonEmptyDirectory(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result = s3Client.listObjectsV2(request);
        return result.getKeyCount() > 0;
    }

    @Override
    public void cleanupPrefix(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket, prefix).getObjectSummaries();
        log.info("Deleting " + CollectionUtils.size(objects) + " s3 objects under " + prefix + " from " + bucket);
        for (S3ObjectSummary summary : objects) {
            s3Client.deleteObject(bucket, summary.getKey());
        }
        s3Client.deleteObject(bucket, prefix);
    }

    @Override
    public void cleanupPrefixByPattern(String bucket, String prefix, String pattern) {
        prefix = sanitizePathToKey(prefix);
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket, prefix).getObjectSummaries();
        Pattern ptn = Pattern.compile(pattern);
        for (S3ObjectSummary summary : objects) {
            String objKey = summary.getKey();
            if (ptn.matcher(objKey).matches()) {
                log.info("Deleting s3 object " + objKey + " from " + bucket);
                s3Client.deleteObject(bucket, objKey);
            }
        }
    }

    @Override
    public List<S3ObjectSummary> listObjects(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        ListObjectsV2Request request = new ListObjectsV2Request() //
                .withBucketName(bucket) //
                .withPrefix(prefix) //
                .withMaxKeys(Integer.MAX_VALUE);
        return s3Client.listObjectsV2(request).getObjectSummaries();
    }

    @Override
    public List<String> listSubFolders(String bucket, String parentDir) {
        List<S3ObjectSummary> summaries = listObjects(bucket, parentDir);
        return summaries.stream() //
                .map(obj -> {
                    String absolutePath = obj.getKey();
                    String relativePath = absolutePath.replace(parentDir + "/", "");
                    if (StringUtils.isNotBlank(relativePath) && relativePath.contains("/")) {
                        return relativePath.substring(0, relativePath.indexOf("/"));
                    } else {
                        return "";
                    }
                }) //
                .filter(StringUtils::isNotBlank) //
                .distinct().collect(Collectors.toList());
    }

    @SuppressWarnings("deprecation")
    @Override
    public MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync) {
        prefix = sanitizePathToKey(prefix);
        TransferManager tm = new TransferManager(s3Client, Executors.newFixedThreadPool(8));
        final MultipleFileUpload upload = tm.uploadDirectory(bucket, prefix, new File(localDir), true);
        final AtomicInteger uploadedObjects = new AtomicInteger(0);

        ExecutorService waiters = Executors.newFixedThreadPool(8);
        final Integer numFiles = upload.getSubTransfers().size();
        log.info("Submitted an upload job for " + numFiles + " files.");
        if (sync) {
            for (final Upload subUpload : upload.getSubTransfers()) {
                waiters.execute(() -> {
                    try {
                        log.info(subUpload.getDescription());
                        subUpload.waitForCompletion();
                        log.info("Uploaded " + uploadedObjects.incrementAndGet() + " out of " + numFiles + " files.");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
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
    public void uploadInputStream(String bucket, String key, InputStream inputStream, Boolean sync) {
        key = sanitizePathToKey(key);
        PutObjectRequest request = new PutObjectRequest(bucket, key, inputStream, null).withCannedAcl(ACL);
        s3Client().putObject(request);
    }

    @Override
    public void uploadLocalFile(String bucket, String key, File file, Boolean sync) {
        key = sanitizePathToKey(key);
        PutObjectRequest request = new PutObjectRequest(bucket, key, file).withCannedAcl(ACL);
        s3Client().putObject(request);
        log.info("Uploaded " + key);
    }

    @Override
    public void createFolder(String bucketName, String folderName) {
        folderName = sanitizePathToKey(folderName);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + "/", emptyContent, metadata);
        putObjectRequest.setCannedAcl(ACL);
        s3Client.putObject(putObjectRequest);
    }

    private void setAclRecursive(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        for (S3ObjectSummary summary : listObjects(bucket, prefix)) {
            s3Client.setObjectAcl(summary.getBucketName(), summary.getKey(), ACL);
        }
    }

    @Override
    public void changeKeyRecursive(String bucket, String srcFolder, String tgtFolder, final String keyId) {
        srcFolder = sanitizePathToKey(srcFolder);
        tgtFolder = sanitizePathToKey(tgtFolder);
        List<Runnable> runnables = new ArrayList<>();
        for (S3ObjectSummary summary : listObjects(bucket, srcFolder)) {
            final String srcKey = summary.getKey();
            if (!srcKey.endsWith("_$folder$")) {
                String subKey = srcKey.substring(srcFolder.length() + 1);
                final String destKey = tgtFolder + "/" + subKey;
                // Make a copy of the object and use server-side encryption when
                // storing the copy.
                CopyObjectRequest request;
                if (StringUtils.isNotBlank(keyId)) {
                    request = new CopyObjectRequest(bucket, srcKey, bucket, destKey).withSSEAwsKeyManagementParams(
                            new SSEAwsKeyManagementParams("0e9daa04-1400-4e55-88c5-b238a9d01721"));
                } else {
                    request = new CopyObjectRequest(bucket, srcKey, bucket, destKey);
                }
                runnables.add(() -> {
                    s3Client.copyObject(request);
                    log.info(String.format("Copied %s to %s using key %s", srcKey, destKey, keyId));
                });
            }
        }
        ThreadPoolUtils.runRunnablesInParallel(workers(), runnables, 600, 1);
    }

    @Override
    public void downloadS3File(S3ObjectSummary itemDesc, File file) throws Exception {
        byte[] buf = new byte[16384];
        try (S3ObjectInputStream stream = s3Client.getObject(itemDesc.getBucketName(), itemDesc.getKey())
                .getObjectContent()) {
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
        object = sanitizePathToKey(object);
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

    @Override
    public URL generateReadUrl(@NotNull String bucket, @NotNull String key, Date expireAt) {
        Preconditions.checkNotNull(bucket);
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(expireAt);
        key = sanitizePathToKey(key);
        // only allow read access
        return s3Client().generatePresignedUrl(bucket, key, expireAt, HttpMethod.GET);
    }

    @Override
    public String getBucketPolicy(String bucket) {
        BucketPolicy bucketPolicy = s3Client().getBucketPolicy(bucket);
        return bucketPolicy.getPolicyText();
    }

    @Override
    public void setBucketPolicy(String bucket, String policyDoc) {
        s3Client().setBucketPolicy(bucket, policyDoc);
    }

    @Override
    public void deleteBucketPolicy(String bucket) {
        s3Client().deleteBucketPolicy(bucket);
    }

    private AmazonS3 s3Client() {
        return s3Client;
    }

    private static ExecutorService workers() {
        if (workers == null) {
            initializeWorkers();
        }
        return workers;
    }

    private synchronized static void initializeWorkers() {
        if (workers == null) {
            workers = ThreadPoolUtils.getFixedSizeThreadPool("s3-service", 8);
        }
    }

    private static String sanitizePathToKey(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        while (path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path;
    }

    @Override
    public List<String> getFilesForDir(String s3Bucket, String prefix) {
        final String delimiter = "/";
        List<String> paths = new LinkedList<>();
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(s3Bucket).withPrefix(prefix);
        ObjectListing result = s3Client.listObjects(request);
        for (S3ObjectSummary summary : result.getObjectSummaries()) {
            if (!summary.getKey().endsWith(delimiter)) {
                paths.add(summary.getKey());
            }
        }
        return paths;
    }
}
