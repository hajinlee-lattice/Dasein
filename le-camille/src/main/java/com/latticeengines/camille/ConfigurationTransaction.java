package com.latticeengines.camille;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public abstract class ConfigurationTransaction {
    public abstract void check(Path path, Document document);
    public abstract void create(Path path, Document document);
    public abstract void set(Path path, Document document);
    public abstract void delete(Path path);
    public abstract void commit();
}
