package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationCacheInterface<T extends ConfigurationScope> {
    void rebuild() throws Exception;

    Document get();
}
