package com.latticeengines.camille;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationTransactionImplFactory {
    public static <T extends ConfigurationScope> ConfigurationTransactionImpl<T> getImplementation(T scope) {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            return null;
        }
        return new StandardConfigurationTransactionImpl<T>(scope);      
    }
}
