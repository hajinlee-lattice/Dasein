package com.latticeengines.skald.model;

public enum FieldInterpretation {
    // Uniquely identifies this record in an external system.
    RecordID,

    // Email address tied to this record.
    EmailAddress,

    // Input feature for the predictive model.
    Feature
}
