package com.latticeengines.redshiftdb.load;

public enum LoadTestStatementType {

    Query_Numeric("QueryLoadTest", "NumericColumnGreaterThanQuery.sql"), //
    Query_String("QueryLoadTest", "StringColumnLikeQuery.sql"), //
    DDL_Add("AlterDDLLoadTest", "AddColumnDDL.sql"), //
    DDL_Drop("AlterDDLLoadTest", "DropColumnDDL.sql");

    private final String testerType;
    private final String scriptFileName;

    LoadTestStatementType(String type, String resourceFileName) {
        this.testerType = type;
        this.scriptFileName = resourceFileName;
    }

    public String getTesterType() {
        return testerType;
    }

    public String getScriptFileName() {
        return scriptFileName;
    }
}
