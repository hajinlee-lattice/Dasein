package com.latticeengines.domain.exposed.dataloader;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryDataResult {

    private boolean success;
    private String errorMessage;
    private List<QueryResultColumn> columns;
    private String dataTable;
    private int remainingRows;

    @JsonProperty("Success")
    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Columns")
    public List<QueryResultColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<QueryResultColumn> columns) {
        this.columns = columns;
    }

    @JsonProperty("DataTable")
    public String getDataTable() {
        return dataTable;
    }

    public void setDataTable(String dataTable) {
        this.dataTable = dataTable;
    }

    @JsonProperty("RemainingRows")
    public int getRemainingRows() {
        return remainingRows;
    }

    public void setRemainingRows(int remainingRows) {
        this.remainingRows = remainingRows;
    }

    public static class QueryResultColumn {

        private String columnName;
        private VdbDataType dataType;
        private short dataLength;
        private List<String> values;

        @JsonProperty("ColumnName")
        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        @JsonProperty("DataType")
        public VdbDataType getDataType() {
            return dataType;
        }

        public void setVdbDataType(VdbDataType dataType) {
            this.dataType = dataType;
        }

        @JsonProperty("DataLength")
        public short getDataLength() {
            return dataLength;
        }

        public void setDataLength(short dataLength) {
            this.dataLength = dataLength;
        }

        @JsonProperty("Values")
        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }
    }

    public static enum VdbDataType {
        Bit(0),
        Byte(1),
        Short(2),
        Int(3),
        Long(4),
        Float(5),
        Double(6),
        VarChar(7),
        NVarChar(8),
        Date(9),
        DateTime(10),
        DateTimeOffset(11),
        Binary(12);

        private int type;

        VdbDataType(int type) {
            this.type = type;
        }

        public int getValue() {
            return type;
        }

        public static VdbDataType valueOf(int value) {
            VdbDataType result = null;
            for (VdbDataType type : VdbDataType.values()) {
                if (value == type.getValue()) {
                    result = type;
                    break;
                }
            }
            return result;
        }
    }
}
