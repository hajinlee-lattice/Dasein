package com.latticeengines.aws.s3;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.Upload;

public interface S3Service {

    void cleanupPrefix(String bucket, String prefix);

    List<S3ObjectSummary> listObjects(String bucket, String prefix);

    MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync);

    Upload uploadLocalFile(String bucket, String key, File file, Boolean sync);

    Upload uploadInputStream(String bucket, String key, InputStream inputStream, Boolean sync);

    void downloadS3File(S3ObjectSummary itemDesc, File file) throws Exception;

    InputStream readObjectAsStream(String bucket, String objectKey);

    boolean objectExist(String bucket, String object);

    boolean isNonEmptyDirectory(String bucket, String prefix);

    void changeKeyRecursive(String bucket, String srcFolder, String tgtFolder, String keyId);
}
