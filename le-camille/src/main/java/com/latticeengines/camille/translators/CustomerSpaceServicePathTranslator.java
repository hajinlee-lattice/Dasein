package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServicePathTranslator extends PathTranslator {
    private CustomerSpaceServiceScope scope;

    public CustomerSpaceServicePathTranslator(CustomerSpaceServiceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) throws Exception {
        String spaceId = scope.getSpaceID();
        if (spaceId == null) {
            spaceId = TenantLifecycleManager.getDefaultSpaceId(scope.getContractID(), scope.getTenantID());
        }
        return PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), scope.getContractID(),
                scope.getTenantID(), spaceId, scope.getServiceName()).append(p);
    }
}
