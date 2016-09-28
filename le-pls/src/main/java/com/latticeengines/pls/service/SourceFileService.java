package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SourceFileService {

    SourceFile findByName(String name);
    
    SourceFile findByTableName(String tableName);

    void create(SourceFile sourceFile);

    void update(SourceFile sourceFile);

    void delete(SourceFile sourceFile);

    SourceFile findByApplicationId(String applicationId);

    void copySourceFile(String tableName, SourceFile originalSourceFile, Tenant targetTenant);
}
