package com.latticeengines.domain.exposed.scoringapi;

public enum FieldInterpretation {

    Feature, // Input feature for the predictive model.
    Date, // A Date field
    Id(FieldType.STRING, "Id"), //
    Event, //
    Domain, //
    FirstName, //
    LastName, //
    Title, //
    Email(FieldType.STRING, "Email"), //
    City(FieldType.STRING, "City"), //
    State(FieldType.STRING, "State"), //
    PostalCode(FieldType.STRING, "PostalCode"), //
    Country(FieldType.STRING, "Country"), //
    PhoneNumber(FieldType.STRING, "PhoneNumber"), //
    Website(FieldType.STRING, "Website"), //
    CompanyName(FieldType.STRING, "CompanyName"), //
    Industry, //
    DUNS(FieldType.STRING, "DUNS"), //
    LatticeAccountId; //

    private final String fieldName; // Added this field, incase if actual
                                    // fieldName contains spaces, we can
                                    // configure it.
    private final FieldType fieldType;
    private final String displayName;

    FieldInterpretation() {
        this.fieldName = this.name();
        this.fieldType = FieldType.STRING;
        this.displayName = null;
    }

    FieldInterpretation(FieldType fieldType) {
        this.fieldName = this.name();
        this.fieldType = fieldType;
        this.displayName = null;
    }

    FieldInterpretation(FieldType fieldType, String displayName) {
        this.fieldName = this.name();
        this.fieldType = fieldType;
        this.displayName = displayName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public String getDisplayName() {
        return displayName;
    }

}
