package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCacheControllerImplFactory {
    public static <T extends ConfigurationScope> ConfigurationCacheControllerImpl<T> getImplementation(T scope) {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            return null;
        }
        return new StandardConfigurationCacheControllerImpl<T>(scope);
    }
}
