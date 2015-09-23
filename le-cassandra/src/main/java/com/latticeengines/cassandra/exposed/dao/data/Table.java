package com.latticeengines.cassandra.exposed.dao.data;

import java.util.ArrayList;
import java.util.List;

public class Table {

    private String tableName;
    private List<Column> partitionColumns = new ArrayList<>(1);
    private List<Column> clusterColumns = new ArrayList<>(1);
    private List<Column> normalColumns = new ArrayList<>(5);

    public Table() {
    }

    public Table(String tableName) {
        this.tableName = tableName;
    }

    public static Table newInstance(String tableName) {
        return new Table(tableName);
    }

    public Table addPartitionColumn(Column column) {
        partitionColumns.add(column);
        return this;
    }

    public Table addClusterColumn(Column column) {
        clusterColumns.add(column);
        return this;
    }

    public Table addNormalColumn(Column column) {
        normalColumns.add(column);
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public List<Column> getClusterColumns() {
        return clusterColumns;
    }

    public List<Column> getNormalColumns() {
        return normalColumns;
    }

}
