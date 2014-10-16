package com.latticeengines.camille.config.bootstrap;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface Installer {
    /**
     * NOTE: It is not guaranteed that this method will be invoked synchronously.
     */
    public DocumentDirectory getInitialConfiguration(int dataVersion);
}
