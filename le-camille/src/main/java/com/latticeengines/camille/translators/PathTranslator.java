package com.latticeengines.camille.translators;

import com.latticeengines.domain.exposed.camille.Path;

public abstract class PathTranslator {
    public abstract Path getAbsolutePath(Path p) throws Exception;
}
