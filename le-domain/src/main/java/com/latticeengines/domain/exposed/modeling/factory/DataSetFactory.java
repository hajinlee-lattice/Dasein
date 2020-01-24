package com.latticeengines.domain.exposed.modeling.factory;

public final class DataSetFactory {

    protected DataSetFactory() {
        throw new UnsupportedOperationException();
    }

    public static final String DATASET_NAME_KEY = "dataset.name";
    public static final String DATASET_INDUSTRY_KEY = "dataset.industry";
    public static final String DATASET_TYPE_KEY = "dataset.type";
    public static final String DATASET_SCHEMA_INTERPRETATION_KEY = "dataset.schema.interpretation";
    public static final String DATASET_TENANT_ID_KEY = "dataset.tenant.id";
    public static final String DATASET_TRAINING_HDFS_PATH_KEY = "dataset.training.hdfs.path";
    public static final String DATASET_TEST_HDFS_PATH_KEY = "dataset.test.hdfs.path";

}
