package com.latticeengines.release.exposed.domain;

public class JenkinsBuildStatus {

    private boolean isBuilding;

    private String result;

    private long number;

    public JenkinsBuildStatus(boolean isBuilding, String result, long number) {
        this.isBuilding = isBuilding;
        this.result = result;
        this.number = number;
    }

    public boolean getIsBuilding() {
        return isBuilding;
    }

    public void setIsBuilding(boolean isBuilding) {
        this.isBuilding = isBuilding;
    }

    public String getResult() {
        return this.result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public long getNumber() {
        return this.number;
    }

    public void setNumber(long number) {
        this.number = number;
    }
}
