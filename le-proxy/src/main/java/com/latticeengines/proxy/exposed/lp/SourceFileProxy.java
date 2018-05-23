package com.latticeengines.proxy.exposed.lp;

import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileProxy {

    SourceFile findByName(String customerSpace, String name);

    SourceFile findByTableName(String customerSpace, String tableName);

    void create(String customerSpace, SourceFile sourceFile);

    void update(String customerSpace, SourceFile sourceFile);

    void delete(String customerSpace, String sourceFileName);

    SourceFile findByApplicationId(String customerSpace, String applicationId);

    void copySourceFile(String customerSpace, String originalSourceFileName, String targetTableName, String targetTenant);

    SourceFile findByTableNameCrossTenant(String tableName);

}
