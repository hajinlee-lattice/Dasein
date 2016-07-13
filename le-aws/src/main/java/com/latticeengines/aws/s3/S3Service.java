package com.latticeengines.aws.s3;

import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;

public interface S3Service {

    void cleanupPrefix(String bucket, String prefix);

    List<S3ObjectSummary> listObjects(String bucket, String prefix);

    MultipleFileUpload uploadLocalDirectory(String bucket, String prefix, String localDir, Boolean sync);

}
