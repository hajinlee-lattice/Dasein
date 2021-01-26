package com.latticeengines.apps.lp.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.CopySourceFileRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileService {

    SourceFile findByName(String name);

    SourceFile findByTableName(String tableName);

    void create(SourceFile sourceFile);

    void update(SourceFile sourceFile);

    void delete(String name);

    SourceFile findByApplicationId(String applicationId);

    void copySourceFile(CopySourceFileRequest request);

    SourceFile getByTableNameCrossTenant(String name);

    List<SourceFile> findAllSourceFiles();

    SourceFile createSourceFileFromS3(String customerSpace, FileProperty fileProperty, String entity, String schema);

    SourceFile findByWorkflowPid(Long workflowPid);
}
