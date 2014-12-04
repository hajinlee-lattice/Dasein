package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class ConfigurationCache<T extends ConfigurationScope> implements ConfigurationCacheInterface<T> {
    private ConfigurationCacheInterface<T> impl;

    @SuppressWarnings("unchecked")
    public ConfigurationCache(T scope, Path relativePath) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            impl = (ConfigurationCacheInterface<T>) new CustomerSpaceServiceConfigurationCacheImpl(
                    (CustomerSpaceServiceScope) scope, relativePath);
        } else {
            impl = new StandardConfigurationCacheImpl<T>(scope, relativePath);
        }
    }

    @Override
    public void rebuild() throws Exception {
        impl.rebuild();
    }

    @Override
    public Document get() {
        return impl.get();
    }
}