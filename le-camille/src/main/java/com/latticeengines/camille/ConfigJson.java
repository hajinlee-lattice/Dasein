package com.latticeengines.camille;

public class ConfigJson {
    private String podId = null;
    private String connectionString = null;

    public ConfigJson() {
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }
}
