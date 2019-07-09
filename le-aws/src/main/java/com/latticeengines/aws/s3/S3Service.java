package com.latticeengines.aws.s3;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public interface S3Service {

    void cleanupPrefix(String bucket, String prefix);

    void cleanupPrefixByDateBetween(String bucket, String prefix, Date start, Date end);

    void cleanupPrefixByPattern(String bucket, String prefix, String pattern);

    List<S3ObjectSummary> listObjects(String bucket, String prefix);

    List<String> listSubFolders(String bucket, String parentDir);

    MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync);

    void uploadLocalFile(String bucket, String key, File file, Boolean sync);

    void uploadInputStream(String bucket, String key, InputStream inputStream, Boolean sync);

    void uploadInputStreamMultiPart(String bucket, String key, InputStream inputStream, long streamLength);

    void createFolder(String bucketName, String folderName);

    void downloadS3File(S3ObjectSummary itemDesc, File file) throws Exception;

    InputStream readObjectAsStream(String bucket, String objectKey);

    boolean objectExist(String bucket, String object);

    void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey);

    void copyLargeObjects(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey);

    void moveObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey);

    boolean isNonEmptyDirectory(String bucket, String prefix);

    void changeKeyRecursive(String bucket, String srcFolder, String tgtFolder, String keyId);

    String getBucketPolicy(String bucket);

    void setBucketPolicy(String bucket, String policyDoc);

    void deleteBucketPolicy(String bucket);

    void addTagToObject(String bucket, String key, String tagKey, String tagValue);

    List<Tag> getObjectTags(String bucket, String key);

    ObjectMetadata getObjectMetadata(String bucket, String key);

    List<String> getFilesForDir(String s3Bucket, String prefix);

    List<S3ObjectSummary> getFilesWithInfoForDir(String s3Bucket, String prefix);

    /**
     * Generate a read only URL to access the specified key under the input bucket. The URL will expires at
     * the given date.
     *
     * @param bucket specified S3 bucket, should not be {@literal null}
     * @param key object key, should not be {@literal null}
     * @param expireAt date where the generated url will expire
     * @return generated URL that have read access to the specified object
     */
    URL generateReadUrl(@NotNull String bucket, @NotNull String key, @NotNull Date expireAt);
}
