package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServicePathTranslator extends PathTranslator {
    private final CustomerSpaceServiceScope scope;

    public CustomerSpaceServicePathTranslator(CustomerSpaceServiceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) throws Exception {
        return PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), scope.getContractId(),
                scope.getTenantId(), scope.getSpaceId(), scope.getServiceName()).append(p);
    }
}
