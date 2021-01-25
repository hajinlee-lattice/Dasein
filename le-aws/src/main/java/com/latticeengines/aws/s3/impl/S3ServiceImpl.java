package com.latticeengines.aws.s3.impl;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.UploadImpl;
import com.amazonaws.services.s3.transfer.internal.UploadMonitor;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@Component("s3Service")
public class S3ServiceImpl implements S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3ServiceImpl.class);
    private static final CannedAccessControlList ACL = CannedAccessControlList.BucketOwnerFullControl;

    private static final long SIZE_LIMIT = 5L * 1024L * 1024L * 1024L;

    private static final long MB = 1024L * 1024L;

    private static final int DEFAULT_ITEM_COUNTS = 4000;
    private static final int MAX_ITEM_COUNTS = 30000;

    @Inject
    private AmazonS3 s3Client;

    @Value("${aws.s3.copy.part.size}")
    private long partSize;
    
    private final RetryTemplate s3ExceptionRetryTemplate = RetryUtils.getRetryTemplate(3,
            Collections.singletonList(AmazonS3Exception.class), null);

    @Override
    public boolean objectExist(String bucket, String object) {
        String key = sanitizePathToKey(object, false);
        return s3Client.doesObjectExist(bucket, key);
    }

    @Override
    public void setObjectAclToBucketOwner(String bucket, String object) {
        s3Client.setObjectAcl(bucket, object, ACL);
    }

    @Override
    public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
            String destinationKey) {
        sourceKey = sanitizePathToKey(sourceKey);
        destinationKey = sanitizePathToKey(destinationKey);
        ObjectMetadata metadataResult = s3Client.getObjectMetadata(sourceBucketName, sourceKey);
        if (metadataResult.getContentLength() >= SIZE_LIMIT) {
            copyLargeObjects(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        } else {
            s3Client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        }
    }

    @Override
    public void copyLargeObjects(String sourceBucketName, String sourceKey, String destinationBucketName,
                                  String destinationKey) {
        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();

        Copy copy = tm.copy(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        try {
            copy.waitForCompletion();
        } catch (InterruptedException e) {
            log.error("Waiting for copy completion interrupted!");
        }
    }

    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }

    @Override
    public void moveObject(String sourceBucketName, String sourceKey, String destinationBucketName,
            String destinationKey) {
        sourceKey = sanitizePathToKey(sourceKey);
        destinationKey = sanitizePathToKey(destinationKey);
        copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        s3Client.deleteObject(sourceBucketName, sourceKey);
    }

    @Override
    public boolean isNonEmptyDirectory(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        ListObjectsV2Request request = new ListObjectsV2Request() //
                .withBucketName(bucket) //
                .withPrefix(StringUtils.appendIfMissing(prefix, "/"));
        ListObjectsV2Result result = s3Client.listObjectsV2(request);
        return result.getKeyCount() > 0;
    }

    @Override
    public void cleanupDirectory(String bucket, String dirPath) {
        dirPath = sanitizePathToKey(dirPath);
        // add trailing space so that objects in other "directory" with the same prefix
        // won't be deleted. E.g., dir_1/file.txt when deleting directory dir/
        String path = StringUtils.appendIfMissing(dirPath, "/");
        ListObjectsV2Request request = new ListObjectsV2Request() //
                .withBucketName(bucket) //
                .withPrefix(path);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            deleteObjects(bucket, dirPath, result);
            if (result.isTruncated()) {
                request = new ListObjectsV2Request() //
                        .withBucketName(bucket) //
                        .withContinuationToken(result.getContinuationToken()) //
                        .withPrefix(path);
            }
        } while (result.isTruncated());
        // delete object without trailing slash
        s3Client.deleteObject(bucket, dirPath);
    }

    @Override
    public void cleanupByObjectList(List<S3ObjectSummary> summaries) {
        if (CollectionUtils.isNotEmpty(summaries)) {
            Map<String, List<String>> bucketKeyMap = summaries.stream()
                            .collect(Collectors.groupingBy(S3ObjectSummary::getBucketName,
                                    Collectors.mapping(S3ObjectSummary::getKey, Collectors.toList())));
            bucketKeyMap.forEach((bucket, keys) -> {
                String[] keyArr = keys.toArray(new String[0]);
                DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket).withKeys(keyArr);
                s3Client.deleteObjects(dor);
            });
        }
    }

    @Override
    public void cleanupPrefixByDateBetween(String bucket, String prefix, Date start, Date end) {
        prefix = sanitizePathToKey(prefix);
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket, prefix).getObjectSummaries();
        log.info("Deleting s3 objects under " + prefix + " from " + bucket + "between " + start + " and " + end);
        List<String> objectKeysToDelete = new ArrayList<>();
        for (S3ObjectSummary summary : objects) {
            if (summary.getLastModified().before(end) && summary.getLastModified().after(start)) {
                objectKeysToDelete.add(summary.getKey());
            }
        }
        if (CollectionUtils.isNotEmpty(objectKeysToDelete)) {
            String[] keys = objectKeysToDelete.toArray(new String[0]);
            DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket).withKeys(keys);
            s3Client.deleteObjects(dor);
        }
    }

    @Override
    public void cleanupPrefixByPattern(String bucket, String prefix, String pattern) {
        prefix = sanitizePathToKey(prefix);
        ListObjectsV2Result result;
        ListObjectsV2Request request = new ListObjectsV2Request() //
                .withBucketName(bucket) //
                .withPrefix(prefix);
        List<String> keysToBeDeleted = new ArrayList<>();
        do {
            result = s3Client.listObjectsV2(request);
            List<S3ObjectSummary> objects = result.getObjectSummaries();
            Pattern ptn = Pattern.compile(pattern);
            for (S3ObjectSummary summary : objects) {
                String objKey = summary.getKey();
                if (ptn.matcher(objKey).matches()) {
                    log.info("Deleting s3 object " + objKey + " from " + bucket);
                    keysToBeDeleted.add(objKey);
                }
            }
            if (CollectionUtils.isNotEmpty(keysToBeDeleted)) {
                String[] keys = keysToBeDeleted.toArray(new String[0]);
                DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket).withKeys(keys);
                s3Client.deleteObjects(dor);
                keysToBeDeleted.clear();
            }
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
    }

    @Override
    public List<S3ObjectSummary> listObjects(String bucket, String prefix) {
        return listObjects(bucket, prefix, DEFAULT_ITEM_COUNTS);
    }

    @Override
    public List<S3ObjectSummary> listObjects(String bucket, String prefix, int maxCount) {
        Preconditions.checkState(maxCount > 0);
        Preconditions.checkState(maxCount <= MAX_ITEM_COUNTS, "Max s3 objects supported in memory is " + MAX_ITEM_COUNTS);
        prefix = sanitizePathToKey(prefix);
        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, bucket, prefix)) {
            objectSummaries.add(summary);
            if (objectSummaries.size() > maxCount) {
                throw new IllegalStateException(String.format("S3 object counts exceed memory count limit (%d), consider " +
                        "expand count or use S3 object iterator!", maxCount));
            }
        }
        return objectSummaries;
    }

    private List<String> getCommonPrefixes(String bucket, String prefix) {
        prefix = sanitizePathToKey(prefix);
        prefix += "/";
        ListObjectsV2Request request = new ListObjectsV2Request() //
                .withBucketName(bucket) //
                .withPrefix(prefix) //
                .withDelimiter("/");
        ListObjectsV2Result result;
        List<String> commonPrefixes = new ArrayList<>();
        do {
            result = s3Client.listObjectsV2(request);
            commonPrefixes.addAll(result.getCommonPrefixes());
            if (commonPrefixes.size() > MAX_ITEM_COUNTS) {
                throw new IllegalStateException(String.format("S3 object counts exceed memory count limit (%d), " +
                        "please use S3 object iterator!", MAX_ITEM_COUNTS));
            }
            String token = result.getNextContinuationToken();
            request.setContinuationToken(token);
        } while (result.isTruncated());
        return commonPrefixes;
    }

    @Override
    public List<String> listSubFolders(String bucket, String parentDir) {
        List<String> prefixes = getCommonPrefixes(bucket, parentDir);
        return prefixes.stream()
                .map(obj -> {
                    String relativePath = obj.replace(parentDir + "/", "");
                    if (StringUtils.isNotBlank(relativePath) && relativePath.contains("/")) {
                        return relativePath.substring(0, relativePath.indexOf("/"));
                    } else {
                        return "";
                    }
                })
                .filter(StringUtils::isNotBlank)
                .distinct().collect(Collectors.toList());
    }

    private void deleteObjects(@NotNull String bucket, @NotNull String dirPath, ListObjectsV2Result result) {
        if (result == null || CollectionUtils.isEmpty(result.getObjectSummaries())) {
            return;
        }

        List<S3ObjectSummary> objects = result.getObjectSummaries();
        log.info("Deleting " + CollectionUtils.size(objects) + " s3 objects under " + dirPath + " from " + bucket);
        String[] keys = objects.stream().map(S3ObjectSummary::getKey).toArray(String[]::new);
        DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket).withKeys(keys);
        s3Client.deleteObjects(dor);
    }

    private void waitForUploadResult(Upload upload, MutableInt uploadedObjects, int numFiles, List<File> failedUploadFiles) {
        try {
            log.info(upload.getDescription());
            upload.waitForCompletion();
            log.info("Uploaded " + uploadedObjects.incrementAndGet() + " out of " + numFiles + " files.");
        } catch (AmazonClientException | InterruptedException e) {
            UploadImpl uploadImpl = (UploadImpl) upload;
            UploadMonitor uploadMonitor = (UploadMonitor) uploadImpl.getMonitor();
            try {
                Field field = uploadMonitor.getClass().getDeclaredField("origReq");
                field.setAccessible(true);
                PutObjectRequest putObjectRequest = (PutObjectRequest) field.get(uploadMonitor);
                failedUploadFiles.add(putObjectRequest.getFile());
                log.info("Failed to upload {} to {}/{} with error {}.", putObjectRequest.getFile().getName(),
                        putObjectRequest.getBucketName(), putObjectRequest.getKey(), e.getMessage());
            } catch (Exception exception) {
                log.error("Can't get file info from upload monitor with error {}.", exception.getMessage());
            }
        }
    }

    private void uploadFileList(String bucket, String finalPrefix, List<File> files, TransferManager tm) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            retry.execute(context -> {
                final MutableInt uploadedObjects = new MutableInt(0);
                final int numFiles = files.size();
                List<File> failedUploadFiles = new ArrayList<>();
                List<Upload> uploads = new ArrayList<>();
                Map<String, File> fileMap =
                        files.stream().collect(Collectors.toMap(file -> finalPrefix + file.getName(), file -> file));
                String[] keysToDelete = fileMap.keySet().toArray(new String[0]);
                DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
                s3Client.deleteObjects(dor);
                fileMap.forEach((key, file) -> uploads.add(tm.upload(bucket, key, file)));
                log.info("Submitted upload jobs for " + numFiles + " files.");
                for (Upload upload : uploads) {
                    waitForUploadResult(upload, uploadedObjects, numFiles, failedUploadFiles);
                }
                if (CollectionUtils.isNotEmpty(failedUploadFiles)) {
                    log.info("{} files failed to upload to {}/{}, need to retry upload.", files.size(), bucket, finalPrefix);
                    files.clear();
                    files.addAll(failedUploadFiles);
                    // retry the failed list
                    throw new RuntimeException();
                }
                return true;
            });
        } catch (Exception e) {
            log.info("There are still {} files failed to upload to {}/{} after retry upload 3 times.", files.size(), bucket, finalPrefix);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync) {
        prefix = sanitizePathToKey(prefix);
        ExecutorService uploadService = Executors.newFixedThreadPool(8);
        TransferManager tm = new TransferManager(s3Client, uploadService);
        tm.getConfiguration().getMultipartUploadThreshold();
        final MultipleFileUpload upload = tm.uploadDirectory(bucket, prefix, new File(localDir), true);
        final MutableInt uploadedObjects = new MutableInt(0);
        final int numFiles = upload.getSubTransfers().size();
        log.info("Submitted an upload job for " + numFiles + " files.");
        List<File> failedUploadFiles = new ArrayList<>();
        if (sync) {
            for (final Upload subUpload : upload.getSubTransfers()) {
                waitForUploadResult(subUpload, uploadedObjects, numFiles, failedUploadFiles);
            }
            if (CollectionUtils.isNotEmpty(failedUploadFiles)) {
                StringBuffer virtualDirectoryKeyPrefix;
                if (StringUtils.isNotEmpty(prefix)) {
                    virtualDirectoryKeyPrefix = new StringBuffer(prefix);
                    if (!prefix.endsWith("/")) {
                        virtualDirectoryKeyPrefix.append("/");
                    }
                } else {
                    virtualDirectoryKeyPrefix = new StringBuffer();
                }
                uploadFileList(bucket, virtualDirectoryKeyPrefix.toString(), failedUploadFiles, tm);
            }
            setAclRecursive(bucket, prefix);
            tm.shutdownNow(false);
        } else {
            uploadService.shutdown();
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
    public void uploadInputStreamMultiPart(String bucket, String key, InputStream inputStream, long streamLength) {

        long uploadPartSize = 16 * MB;
        int readLimit = (int)(20 * MB);
        List<PartETag> partETags = new ArrayList<>();

        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        // Upload parts.
        long pos = 0;
        for (int i = 1; pos < streamLength; i++) {

            uploadPartSize = Math.min(uploadPartSize, (streamLength - pos));

            // Create the request to upload a part.
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(initResponse.getUploadId())
                    .withPartNumber(i)
                    .withInputStream(inputStream)
                    .withPartSize(uploadPartSize);
            uploadRequest.getRequestClientOptions().setReadLimit(readLimit);
            // Upload the part and add the response's ETag to list.
            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
            partETags.add(uploadResult.getPartETag());
            pos += uploadPartSize;
        }
        // Complete the multipart upload.
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket, key,
                initResponse.getUploadId(), partETags);
        s3Client.completeMultipartUpload(compRequest);
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
        String finalFolderName = folderName;
        s3ExceptionRetryTemplate.execute(ctx -> {
            try {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") create s3 folder: " + finalFolderName);
                }
                s3Client.putObject(putObjectRequest);
            } catch (AmazonS3Exception e) {
                log.error("Encounter S3 Exception: " + e.getErrorCode());
                throw e;
            }
            return true;
        });
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
        ThreadPoolUtils.runInParallel(runnables, 600, 1);
    }

    @Override
    public void downloadS3File(S3ObjectSummary itemDesc, File file) throws Exception {

        try {

            TransferManagerBuilder
                    .standard()
                    .withS3Client(s3Client)
                    .build()
                    .download(itemDesc.getBucketName(), itemDesc.getKey(), file)
                    .waitForCompletion();

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            throw e;

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
    public Iterator<InputStream> getObjectStreamIterator(String bucket, String prefix, S3KeyFilter filter) {
        return new ObjectStreamIterator(bucket, prefix, filter);
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
    public ObjectMetadata getObjectMetadata(String bucket, String key) {
        return s3Client.getObjectMetadata(bucket, sanitizePathToKey(key));
    }

    @Override
    public void addTagToObject(String bucket, String key, String tagKey, String tagValue) {
        if (!objectExist(bucket, key)) {
            return;
        }
        key = sanitizePathToKey(key);
        GetObjectTaggingRequest getTaggingRequest = new GetObjectTaggingRequest(bucket, key);
        GetObjectTaggingResult getTagsResult = s3Client.getObjectTagging(getTaggingRequest);

        Map<String, Tag> tagMap= getTagsResult.getTagSet().stream().collect(Collectors.toMap(Tag::getKey, tag -> tag));
        tagMap.put(tagKey, new Tag(tagKey, tagValue));
        SetObjectTaggingRequest setTaggingRequest = new SetObjectTaggingRequest(bucket, key,
                new ObjectTagging(new ArrayList<>(tagMap.values())));
        s3Client.setObjectTagging(setTaggingRequest);
    }

    @Override
    public List<Tag> getObjectTags(String bucket, String key) {
        if (!objectExist(bucket, key)) {
            return Collections.emptyList();
        }
        key = sanitizePathToKey(key);
        GetObjectTaggingRequest getTaggingRequest = new GetObjectTaggingRequest(bucket, key);
        GetObjectTaggingResult getTagsResult = s3Client.getObjectTagging(getTaggingRequest);
        return getTagsResult.getTagSet();
    }

    @Override
    public void deleteObjectTags(String bucket, String key) {
        DeleteObjectTaggingRequest deleteTaggingRequest = new DeleteObjectTaggingRequest(bucket, key);
        s3Client.deleteObjectTagging(deleteTaggingRequest);
    }

    @Override
    public List<Rule> getBucketLifecycleConfigurationRules(String bucket) {
        BucketLifecycleConfiguration configuration = s3Client.getBucketLifecycleConfiguration(bucket);
        if (configuration != null) {
            return configuration.getRules();
        }
        return null;
    }

    private AmazonS3 s3Client() {
        return s3Client;
    }

    private static String sanitizePathToKey(String path) {
        return sanitizePathToKey(path, true);
    }

    private static String sanitizePathToKey(String path, boolean handleTail) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (handleTail)
        {
            while (path.endsWith("/")) {
                path = path.substring(0, path.lastIndexOf("/"));
            }
        }

        return path;
    }

    @Override
    public List<String> getFilesForDir(String s3Bucket, String prefix) {
        return getFilesForDir(s3Bucket, prefix, DEFAULT_ITEM_COUNTS);
    }

    @Override
    public List<String> getFilesForDir(String s3Bucket, String prefix, int maxCount) {
        Preconditions.checkState(maxCount > 0);
        Preconditions.checkState(maxCount <= MAX_ITEM_COUNTS, "Max s3 objects supported in memory is " + MAX_ITEM_COUNTS);
        final String delimiter = "/";
        List<String> paths = new LinkedList<>();
        for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, s3Bucket, prefix)) {
            if (!summary.getKey().endsWith(delimiter)) {
                paths.add(summary.getKey());
                if (paths.size() > maxCount) {
                    throw new IllegalStateException(String.format("S3 object counts exceed memory count limit (%d), consider " +
                            "expand count or use S3 object iterator!", maxCount));
                }
            }
        }
        return paths;
    }

    @Override
    public List<S3ObjectSummary> getFilesWithInfoForDir(String s3Bucket, String prefix) {
        return getFilesWithInfoForDir(s3Bucket, prefix, DEFAULT_ITEM_COUNTS);
    }

    @Override
    public List<S3ObjectSummary> getFilesWithInfoForDir(String s3Bucket, String prefix, int maxCount) {
        Preconditions.checkState(maxCount > 0);
        Preconditions.checkState(maxCount <= MAX_ITEM_COUNTS, "Max s3 objects supported in memory is " + MAX_ITEM_COUNTS);
        final String delimiter = "/";
        List<S3ObjectSummary> s3ObjectSummaries = new LinkedList<>();
        for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, s3Bucket, prefix)) {
            if (!summary.getKey().endsWith(delimiter)) {
                s3ObjectSummaries.add(summary);
                if (s3ObjectSummaries.size() > maxCount) {
                    throw new IllegalStateException(String.format("S3 object counts exceed memory count limit (%d), consider " +
                            "expand count or use S3 object iterator!", maxCount));
                }
            }
        }
        return s3ObjectSummaries;
    }

    @Override
    public S3Objects getIterableObjects(String s3Bucket, String prefix) {
        return S3Objects.withPrefix(s3Client, s3Bucket, prefix);
    }

    /**
     * Iterator of input stream of S3 objects under specified bucket and prefix
     * which satisfy S3KeyFilter
     */
    private class ObjectStreamIterator implements Iterator<InputStream>, Closeable {

        private String bucket;
        private List<String> objectKeys;
        private int objectIdx = -1;
        private InputStream current;

        ObjectStreamIterator(String bucket, String prefix, S3KeyFilter filter) {
            this.bucket = bucket;
            objectKeys = listObjects(bucket, prefix).stream() //
                    .map(S3ObjectSummary::getKey)
                    .filter(filter::accept)
                    .collect(Collectors.toList());
        }

        @Override
        public void close() {
            closeCurrStream();
        }

        @Override
        public boolean hasNext() {
            return objectKeys != null && objectIdx < objectKeys.size() - 1;
        }

        @Override
        public InputStream next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            closeCurrStream();
            current = readObjectAsStream(bucket, objectKeys.get(++objectIdx));
            return current;
        }

        private void closeCurrStream() {
            if (current == null) {
                return;
            }
            try {
                current.close();
            } catch (IOException e) {
                log.error("Fail to close input stream for S3 object " + objectKeys.get(objectIdx), e);
            } finally {
                current = null;
            }
        }

    }
}
