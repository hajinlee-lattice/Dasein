package com.latticeengines.common.exposed.version;

public class VersionManager {

    private String currentVersion;

    public VersionManager(String currentVersion) {
        this.currentVersion = currentVersion;
    }

    public String getCurrentVersionInStack(String stackName){
        String fullVersion = stackName + "/" + currentVersion;
        if (fullVersion.startsWith("/")) {
            fullVersion = fullVersion.substring(1);
        }
        if (fullVersion.endsWith("/")) {
            fullVersion = fullVersion.substring(0, fullVersion.lastIndexOf("/"));
        }
        return fullVersion;
    }

    public String getCurrentVersion(){
        return getCurrentVersionInStack("");
    }

}
