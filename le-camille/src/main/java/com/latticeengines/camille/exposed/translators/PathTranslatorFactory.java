package com.latticeengines.camille.exposed.translators;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.ContractScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.PodDivisionScope;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class PathTranslatorFactory {
    public static PathTranslator getTranslator(ConfigurationScope scope) {
        switch (scope.getType()) {
        case POD:
            return new PodPathTranslator((PodScope)scope);
        case POD_DIVISION:
            return new PodDivisionPathTranslator((PodDivisionScope)scope);
        case CONTRACT:
            return new ContractPathTranslator((ContractScope)scope);
        case CUSTOMER_SPACE:
            return new CustomerSpacePathTranslator((CustomerSpaceScope)scope);
        case CUSTOMER_SPACE_SERVICE:
            return new CustomerSpaceServicePathTranslator((CustomerSpaceServiceScope)scope);
        case SERVICE:
            return new ServicePathTranslator((ServiceScope)scope);
        case TENANT:
            return new TenantPathTranslator((TenantScope)scope);
        default:
            throw new IllegalArgumentException("Unimplemented scope specified: " + scope.getType());       
        }
    }
}
