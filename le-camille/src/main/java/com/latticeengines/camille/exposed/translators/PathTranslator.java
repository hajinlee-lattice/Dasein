package com.latticeengines.camille.exposed.translators;

import com.latticeengines.domain.exposed.camille.Path;

public abstract class PathTranslator {
    public Path getAbsolutePath(Path p) throws Exception {
        return getBasePath().append(p);
    }

    public abstract Path getBasePath() throws Exception;
}
