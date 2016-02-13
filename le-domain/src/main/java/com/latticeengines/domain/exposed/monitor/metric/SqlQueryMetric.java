package com.latticeengines.domain.exposed.monitor.metric;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class SqlQueryMetric implements Fact, Dimension {

    private String hostName;
    private String tableName;
    private String serverType;
    private Integer rows;
    private Integer cols;
    private Integer timeElapsed;

    public SqlQueryMetric(String hostName, String tableName, String serverType, Integer rows, Integer cols,
            Integer timeElapsed) {
        this.hostName = hostName;
        this.tableName = tableName;
        this.serverType = serverType;
        this.rows = rows;
        this.cols = cols;
        this.timeElapsed = timeElapsed;
    }

    @MetricTag(tag = "HostName")
    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    @MetricTag(tag = "TableName")
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @MetricTag(tag = "ServerType")
    public String getServerType() {
        return serverType;
    }

    public void setServerType(String serverType) {
        this.serverType = serverType;
    }

    @MetricField(name = "Rows", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRows() {
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    @MetricField(name = "Columns", fieldType = MetricField.FieldType.INTEGER)
    public Integer getCols() {
        return cols;
    }

    public void setCols(Integer cols) {
        this.cols = cols;
    }

    @MetricField(name = "TimeElapsed", fieldType = MetricField.FieldType.INTEGER)
    public Integer getTimeElapsed() {
        return timeElapsed;
    }

    public void setTimeElapsed(Integer timeElapsed) {
        this.timeElapsed = timeElapsed;
    }
}
