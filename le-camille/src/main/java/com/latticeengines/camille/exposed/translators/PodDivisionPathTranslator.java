package com.latticeengines.camille.exposed.translators;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodDivisionScope;

public class PodDivisionPathTranslator extends PathTranslator {

    public PodDivisionPathTranslator(PodDivisionScope scope) {
    }

    @Override
    public Path getBasePath() {
        if (StringUtils.isEmpty(CamilleEnvironment.getDivision())) {
            return PathBuilder.buildPodPath(CamilleEnvironment.getPodId());
        }
        return PathBuilder.buildPodDivisionPath(CamilleEnvironment.getPodId(), CamilleEnvironment.getDivision());
    }
}
