package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class ConfigurationCache<T extends ConfigurationScope> implements ConfigurationCacheInterface<T> {
    private ConfigurationCacheInterface<T> impl;

    @SuppressWarnings("unchecked")
    private ConfigurationCache(T scope, Path localPath) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            impl = (ConfigurationCacheInterface<T>) new CustomerSpaceServiceConfigurationCacheImpl(
                    (CustomerSpaceServiceScope) scope, localPath);
        } else {
            impl = new StandardConfigurationCacheImpl<T>(scope, localPath);
        }
    }

    public static <T extends ConfigurationScope> ConfigurationCache<T> construct(T scope, Path localPath)
            throws Exception {
        return new ConfigurationCache<T>(scope, localPath);
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