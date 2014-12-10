package com.latticeengines.camille.exposed.translators;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class TenantPathTranslator extends PathTranslator {
    private TenantScope scope;

    public TenantPathTranslator(TenantScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getBasePath() throws Exception {
        return PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), scope.getContractId(), scope.getTenantId());
    }
}
