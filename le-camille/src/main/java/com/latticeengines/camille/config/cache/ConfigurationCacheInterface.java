package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationCacheInterface<T extends ConfigurationScope> {
    public void rebuild() throws Exception;

    public Document get();
}
