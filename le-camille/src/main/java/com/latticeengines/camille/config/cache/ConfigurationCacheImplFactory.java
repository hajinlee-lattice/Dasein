package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCacheImplFactory {
    public static <T extends ConfigurationScope> ConfigurationCacheImpl<T> getImplementation(T scope,
            Path relativePath) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            return null;
        }
        return new StandardConfigurationCacheImpl<T>(scope, relativePath);
    }
}
