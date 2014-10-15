package com.latticeengines.camille.properties;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class PropertiesManagerImplFactory {
    public static <T extends ConfigurationScope> PropertiesManagerImpl<T> getImplementation(T scope, Path path)
            throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            // TODO
            return null;
        }
        return new StandardPropertiesManagerImpl<T>(scope, path);
    }
}
