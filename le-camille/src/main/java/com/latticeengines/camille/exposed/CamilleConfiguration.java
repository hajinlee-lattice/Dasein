package com.latticeengines.camille.exposed;

import org.apache.commons.lang.StringUtils;

public class CamilleConfiguration {
    private String podId = null;
    private String connectionString = null;
    private String division = null;
    private String sharedQueues = null;
    private String[] sharedQueueList = null;

    public CamilleConfiguration() {
    }

    public CamilleConfiguration(String podId, String connectionString) {
        setPodId(podId);
        setConnectionString(connectionString);
    }

    public CamilleConfiguration(String podId, String connectionString, String division, String sharedQueues) {
        setPodId(podId);
        setConnectionString(connectionString);
        setDivision(division);
        setSharedQueues(sharedQueues);
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

    public String getDivision() {
        return division;
    }

    public void setDivision(String division) {
        this.division = (StringUtils.isEmpty(division)) ? null : division;
    }

    public String getSharedQueues() {
        return sharedQueues;
    }

    public void setSharedQueues(String sharedQueues) {
        this.sharedQueues = (StringUtils.isEmpty(sharedQueues)) ? null : sharedQueues;
        sharedQueueList = (sharedQueues == null) ? null :
                          sharedQueues.split("\\s*,\\s*");
    }

    public boolean isSharedQueue(String queueName) {
        if (division == null) {
            return true;
        } else if (sharedQueueList != null) {
            for (int i = 0; i < sharedQueueList.length; i++) {
                if (queueName.equals(sharedQueueList[i])) {
                    return true;
                }
            }
        }
        return false;
    }
}
