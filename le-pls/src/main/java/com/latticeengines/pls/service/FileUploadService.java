package com.latticeengines.pls.service;

import java.io.InputStream;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface FileUploadService {

    SourceFile uploadFile(String outputFileName, //
            SchemaInterpretation interpretation, //
            String entity, //
            String displayName, //
            InputStream fileInputStream);

    SourceFile uploadFile(String outputFileName, String displayName,
            InputStream fileInputStream);

    Table getMetadata(String fileName);

    InputStream getImportErrorStream(String sourceFileName);

    SourceFile uploadCleanupFileTemplate(SourceFile sourceFile, SchemaInterpretation schemaInterpretation,
                                         CleanupOperationType cleanupOperationType);
}
