package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.TenantScope;

public class TenantPathTranslator extends PathTranslator {
    private TenantScope scope;
    
    public TenantPathTranslator(TenantScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) {
        return PathBuilder.buildTenantPath(CamilleEnvironment.getPodId(), scope.getContractID(), scope.getTenantID());    
    }
}
