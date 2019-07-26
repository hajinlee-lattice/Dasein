package com.latticeengines.aws.s3;

public interface S3KeyFilter {
    default boolean accept(String key) {
        return key != null && key.endsWith(".avro");
    }

}
