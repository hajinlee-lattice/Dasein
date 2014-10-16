package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.camille.config.StandardConfigurationControllerImpl;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCache<T extends ConfigurationScope> implements ConfigurationCacheImpl<T> {
    private ConfigurationCacheImpl<T> impl;

    public ConfigurationCache(T scope, Path relativePath) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            impl = new StandardConfigurationCacheImpl<T>(scope, relativePath);
        }
        else {
            impl = new StandardConfigurationCacheImpl<T>(scope, relativePath);
        }
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
