package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationCacheImpl<T extends ConfigurationScope> {
    public void rebuild() throws Exception;

    public Document get() throws DocumentSerializationException;
}
