package com.latticeengines.common.exposed.version;

public class VersionManager {

    private String currentVersion;

    public VersionManager(String currentVersion) {
        this.currentVersion = currentVersion;
    }

    public String getCurrentVersion(){
        return this.currentVersion;
    }

}
