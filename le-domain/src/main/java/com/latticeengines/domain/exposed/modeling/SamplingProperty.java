package com.latticeengines.domain.exposed.modeling;

public enum SamplingProperty {

    // Bootstrap sampling
    TRAINING_DATA_SIZE, //
    TRAINING_SET_SIZE, //

    // Stratified sampling
    TARGET_COLUMN_NAME, //
    CLASS_DISTRIBUTION, //

    // Up sampling
    // TARGET_COLUMN_NAME
    MINORITY_CLASS_LABEL, //
    MINORITY_CLASS_SIZE, //
    UP_TO_PERCENTAGE, //

    // Down sampling
    // TARGET_COLUMN_NAME
    MAJORITY_CLASS_LABEL, //
    DOWN_TO_PERCENTAGE, //

}