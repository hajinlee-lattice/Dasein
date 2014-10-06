package com.latticeengines.camille;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationControllerImplFactory {
    public static <T extends ConfigurationScope> ConfigurationControllerImpl<T> getImplementation(T scope) {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            return null;
        }
        return new StandardConfigurationControllerImpl<T>(scope);      
    }
}
