package com.latticeengines.camille.translators;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class PodPathTranslator extends PathTranslator {
    public PodPathTranslator(PodScope scope) {
    }

    @Override
    public Path getAbsolutePath(Path p) throws Exception {
        return PathBuilder.buildPodPath(CamilleEnvironment.getPodId()).append(p);
    }
}
