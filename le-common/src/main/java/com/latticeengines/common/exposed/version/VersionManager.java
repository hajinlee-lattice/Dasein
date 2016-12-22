package com.latticeengines.common.exposed.version;

import org.apache.log4j.Logger;

import com.jcabi.manifests.Manifests;

public class VersionManager {

    private static final Logger log = Logger.getLogger(VersionManager.class);

    private String currentVersion;

    public VersionManager(String currentVersion) {
        this.currentVersion = currentVersion;
    }

    public String getCurrentVersionInStack(String stackName) {
        String fullVersion = stackName + "/" + currentVersion;
        if (fullVersion.startsWith("/")) {
            fullVersion = fullVersion.substring(1);
        }
        if (fullVersion.endsWith("/")) {
            fullVersion = fullVersion.substring(0, fullVersion.lastIndexOf("/"));
        }
        return fullVersion;
    }

    public String getCurrentVersion() {
        return getCurrentVersionInStack("");
    }

    public String getCurrentSvnRevision() {
        try {
            return Manifests.read("LE-SCM-Revision");
        } catch (Exception e) {
            log.warn("Failed to read LE-SCM-Revision from Jar manifest");
            return null;
        }
    }
}
