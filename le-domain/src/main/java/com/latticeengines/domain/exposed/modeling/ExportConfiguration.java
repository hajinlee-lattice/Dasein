package com.latticeengines.domain.exposed.modeling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ExportConfiguration {

    private String table;
    private String customer;
    private DbCreds creds;
    private String hdfsDirPath;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public DbCreds getCreds() {
        return creds;
    }

    public void setCreds(DbCreds creds) {
        this.creds = creds;
    }

    @JsonProperty("hdfs_dir_path")
    public String getHdfsDirPath() {
        return hdfsDirPath;
    }

    @JsonProperty("hdfs_dir_path")
    public void setHdfsDirPath(String hdfsDirPath) {
        this.hdfsDirPath = hdfsDirPath;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
