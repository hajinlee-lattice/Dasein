package com.latticeengines.camille.config;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationTransactionInterface<T extends ConfigurationScope> {

    void check(Path path, Document document) throws Exception;

    void create(Path path, Document document) throws Exception;

    void set(Path path, Document document) throws Exception;

    void delete(Path path) throws Exception;

    void commit() throws Exception;
}
