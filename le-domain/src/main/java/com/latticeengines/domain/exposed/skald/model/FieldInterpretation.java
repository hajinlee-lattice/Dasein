package com.latticeengines.domain.exposed.skald.model;

public enum FieldInterpretation {
    // Uniquely identifies this record in an external system.
    RECORD_ID,

    // Email address tied to this record.
    EMAIL_ADDRESS,

    // Input feature for the predictive model.
    FEATURE
}
