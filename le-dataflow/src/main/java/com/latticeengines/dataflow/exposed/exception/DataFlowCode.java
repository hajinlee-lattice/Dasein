package com.latticeengines.dataflow.exposed.exception;

public enum DataFlowCode {
    // Low level errors: 00000-09999
    DF_00000("Builder bean {0} not instance of builder."), //
    // Data flow building: 10000-10999
    DF_10000("Data flow context does not have values for required properties: {0}"), //
    DF_10001("Unknown field name {0} from previous pipe."), //
    DF_10002("Unknown field name {0} from previous pipe {1}."), //
    DF_10003("Unseen prior pipe {0}."), //
    DF_10004("Getting schema failed."), //
    DF_10005("Getting schema failed for path {0}."), //
    DF_10006("Table {0} has no primary key."), //
    DF_10007("Primary key of table {0} has no attributes."), //
    DF_10008("Table has no name."), //
    DF_10009("Extract for table {0} has no name."), //
    DF_10010("Extract {0} for table {1} has no path."), //
    DF_10011("Table {0} has no extracts."); //

    private String message;

    DataFlowCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
