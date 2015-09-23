package com.latticeengines.cassandra.exposed.dao.data;

public class Column {
    private String columnName;
    private ColumnType columnType;
    private Object value;
    private Integer precision;
    private Integer scale;

    public Column(String columnName, ColumnType columnType) {
        super();
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public Column(String columnName, ColumnType columnType, Integer precision, Integer scale) {
        super();
        this.columnName = columnName;
        this.columnType = columnType;
        this.precision = precision;
        this.scale = scale;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

}
