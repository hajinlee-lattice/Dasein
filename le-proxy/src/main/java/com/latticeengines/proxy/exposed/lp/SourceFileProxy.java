package com.latticeengines.proxy.exposed.lp;

import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileProxy {

    SourceFile findByName(String customerSpace, String name);

    SourceFile findByTableName(String customerSpace, String tableName);

    void create(String customerSpace, SourceFile sourceFile);

    void update(String customerSpace, SourceFile sourceFile);

    void delete(String customerSpace, String sourceFileName);

    SourceFile findByApplicationId(String customerSpace, String applicationId);

    SourceFile findByWorkflowPid(String customerSpace, String workflowPid);

    void copySourceFile(String customerSpace, String originalSourceFileName, String targetTableName, String targetTenant);

    SourceFile findByTableNameCrossTenant(String tableName);

    SourceFile createSourceFileFromS3(String customerSpace, FileProperty fileProperty, String entity, String schema);
}
