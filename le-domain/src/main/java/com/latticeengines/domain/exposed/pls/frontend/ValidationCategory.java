package com.latticeengines.domain.exposed.pls.frontend;

public enum ValidationCategory {
    DataType("Data Type","It doesn't look like you have changed any data types."),
    DataFormat("Data Format","Time zone and format appears to be fine"),
    ColumnMapping("Column Mapping",""),
    RequiredField("Required Field","All required fields checked out."),
    Others("Others","It doesn’t look like the file you upload has any consistency issues.");

    private String displayName;
    private String validMessage;

    ValidationCategory(String displayName, String validMessage) {
        this.displayName = displayName;
        this.validMessage = validMessage;
    }

}
