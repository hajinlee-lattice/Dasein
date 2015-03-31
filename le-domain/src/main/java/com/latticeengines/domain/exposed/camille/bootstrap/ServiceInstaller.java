package com.latticeengines.domain.exposed.camille.bootstrap;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceInstaller {
    public DocumentDirectory install(String serviceName, int dataVersion);
}
