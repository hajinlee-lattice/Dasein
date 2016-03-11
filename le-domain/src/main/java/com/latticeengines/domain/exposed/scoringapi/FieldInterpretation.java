package com.latticeengines.domain.exposed.scoringapi;

public enum FieldInterpretation {
    // Input feature for the predictive model.
    FEATURE,
    // Uniquely identifies this record in an external system.
    ID,
    EVENT,
    DOMAIN,
    LAST_MODIFIED_DATE,
    CREATED_DATE,
    FIRST_NAME,
    LAST_NAME,
    TITLE,
    EMAIL_ADDRESS,
    COMPANY_CITY,
    COMPANY_STATE,
    POSTAL_CODE,
    COMPANY_COUNTRY,
    PHONE_NUMBER,
    WEBSITE,
    COMPANY_NAME,
    INDUSTRY
}
