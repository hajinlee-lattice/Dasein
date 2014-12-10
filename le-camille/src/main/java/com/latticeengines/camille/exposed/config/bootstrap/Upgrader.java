package com.latticeengines.camille.exposed.config.bootstrap;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface Upgrader {
    /**
     * NOTE: It is not guaranteed that this method will be invoked synchronously.
     */
    public DocumentDirectory upgradeConfiguration(int sourceVersion, int targetVersion,
            DocumentDirectory source);
}
