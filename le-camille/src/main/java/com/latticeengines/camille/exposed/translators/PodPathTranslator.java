package com.latticeengines.camille.exposed.translators;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class PodPathTranslator extends PathTranslator {
    public PodPathTranslator(PodScope scope) {
    }

    @Override
    public Path getBasePath() throws Exception {
        return PathBuilder.buildPodPath(CamilleEnvironment.getPodId());
    }
}
