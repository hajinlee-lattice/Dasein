package com.latticeengines.domain.exposed.scoringapi;

public enum FieldInterpretation {
    // Uniquely identifies this record in an external system.
    RECORD_ID,

    // Email address tied to this record.
    EMAIL_ADDRESS,
    WEBSITE,
    COMPANY_NAME,
    COMPANY_CITY,
    COMPANY_STATE,
    COMPANY_COUNTRY,

    // Input feature for the predictive model.
    FEATURE
}
