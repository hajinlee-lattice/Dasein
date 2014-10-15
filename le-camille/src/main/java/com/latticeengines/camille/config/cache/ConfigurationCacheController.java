package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCacheController<T extends ConfigurationScope> implements ConfigurationCacheControllerImpl<T> {
    private ConfigurationCacheControllerImpl<T> impl;

    public ConfigurationCacheController(T scope, Path relativePath) throws Exception {
        impl = ConfigurationCacheControllerImplFactory.getImplementation(scope, relativePath);
    }

    @Override
    public void rebuild() throws Exception {
        impl.rebuild();
    }

    @Override
    public Document get() throws DocumentSerializationException {
        return impl.get();
    }
}
