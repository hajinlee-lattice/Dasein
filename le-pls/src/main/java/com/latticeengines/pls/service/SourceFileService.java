package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileService {

    SourceFile findByName(String name);

    void create(SourceFile sourceFile);

    void update(SourceFile sourceFile);

    void delete(SourceFile sourceFile);

    SourceFile findByApplicationId(String applicationId);
}
