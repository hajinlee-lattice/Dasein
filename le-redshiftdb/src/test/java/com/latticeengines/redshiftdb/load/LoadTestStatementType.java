package com.latticeengines.redshiftdb.load;

public enum LoadTestStatementType {

    Query_Numeric("QueryLoadTest", "NumericColumnGreaterThanQuery.sql"), //
    Query_String("QueryLoadTest", "StringColumnLikeQuery.sql"), //
    DDL_Add("AlterDDLLoadTest", "AddColumnDDL.sql"), //
    DDL_Drop("AlterDDLLoadTest", "DropColumnDDL.sql"), //
    Query_Numeric_Join("QueryLoadTest", "NumericColumnJoinQuery.sql"), //
    Query_Numeric_BitMask("QueryLoadTest", "NumericColumnBitMaskQuery.sql"), //
    Query_Multi_Conditional("QueryMultiConditionalTest", "QueryMultiConditional.sql");

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
