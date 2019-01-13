package com.latticeengines.common.exposed.version;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcabi.manifests.Manifests;

public class VersionManager {

    private static final Logger log = LoggerFactory.getLogger(VersionManager.class);

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

    public String getCurrentGitCommit() {
        try {
            return Manifests.read("LE-SCM-Revision");
        } catch (Exception e) {
            log.warn("Failed to read LE-SCM-Revision from Jar manifest");
            return null;
        }
    }
}
