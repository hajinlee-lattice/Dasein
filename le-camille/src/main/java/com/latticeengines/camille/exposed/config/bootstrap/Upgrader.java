package com.latticeengines.camille.exposed.config.bootstrap;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface Upgrader {
    public DocumentDirectory upgradeConfiguration(int sourceVersion, int targetVersion, DocumentDirectory source);
}
