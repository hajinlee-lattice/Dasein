package com.latticeengines.scoringapi.warnings;

public enum WarningCode {

    //@formatter:off
    MISSING_COLUMN("missing_column", "Input record has fewer columns than what the model expects; missing columns: {0}"),
    MISSING_VALUE("missing_value", "Input record has missing values for certain expected columns; columns with missing values: {0}"),
    NO_MATCH("no_match", "No match found for record in Lattice Data Cloud; domain key values: {0}; matched key values: {1}"),
    MISMATCHED_DATATYPE("mismatched_datatype", "Input record contains columns that do not match expected datatypes: {0}"),
    PUBLIC_DOMAIN("public_domain", "A public domain key was passed in for this record: {0}"),
    EXTRA_FIELDS("extra_fields", "Input record contains extra columns: {0}");
    //@formatter:on

    private String externalCode;

    private String description;

    WarningCode(String externalCode, String description) {
        this.externalCode = externalCode;
        this.description = description;
    }

    public String getExternalCode() {
        return externalCode;
    }

    public String getDescription() {
        return description;
    }

}
