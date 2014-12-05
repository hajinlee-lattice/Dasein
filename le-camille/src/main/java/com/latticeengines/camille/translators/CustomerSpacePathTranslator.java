package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;

public class CustomerSpacePathTranslator extends PathTranslator {
    private CustomerSpaceScope scope;

    public CustomerSpacePathTranslator(CustomerSpaceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getBasePath() throws Exception {
        String spaceId = scope.getSpaceId();
        if (spaceId == null) {
            spaceId = TenantLifecycleManager.getDefaultSpaceId(scope.getContractId(), scope.getTenantId());
        }
        return PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), scope.getContractId(),
                scope.getTenantId(), spaceId);
    }
}
