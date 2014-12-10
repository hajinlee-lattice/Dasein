package com.latticeengines.camille.exposed;

public class CamilleConfiguration {
    private String podId = null;
    private String connectionString = null;

    public CamilleConfiguration() {
    }

    public CamilleConfiguration(String podId, String connectionString) {
        setPodId(podId);
        setConnectionString(connectionString);
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
