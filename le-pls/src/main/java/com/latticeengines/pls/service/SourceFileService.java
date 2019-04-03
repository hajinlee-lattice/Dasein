package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileService {

    SourceFile findByName(String name);
    
    SourceFile findByTableName(String tableName);

    void create(SourceFile sourceFile);

    void update(SourceFile sourceFile);

    void delete(SourceFile sourceFile);

    SourceFile findByApplicationId(String applicationId);

    void copySourceFile(String originalSourceFileName, String targetTableName, String targetTenant);

    SourceFile getByTableNameAcrossTenant(String tableName);

    SourceFile createSourceFileFromS3(FileProperty fileProperty,
                                      SchemaInterpretation schemaInterpretation,
                                      String entity);
}
