package com.latticeengines.camille.exposed.config.bootstrap;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface Installer {
    public DocumentDirectory getInitialConfiguration(int dataVersion);
}
