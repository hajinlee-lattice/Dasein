package com.latticeengines.camille.translators;

import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class PodPathTranslator extends PathTranslator {
    private PodScope scope;
    
    public PodPathTranslator(PodScope scope) {
        this.scope = scope;
    }

    @Override
    public Path getAbsolutePath(Path p) {
        return PathBuilder.buildPodPath(scope.getPodID()).append(p);
    }
}
