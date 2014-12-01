package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServicePathTranslator extends PathTranslator {
    private ServiceScope scope;
    
    public ServicePathTranslator(ServiceScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) throws Exception {
        return PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), scope.getServiceName()).append(p);
    }
}
