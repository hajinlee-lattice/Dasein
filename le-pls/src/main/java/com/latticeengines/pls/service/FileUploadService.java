package com.latticeengines.pls.service;

import java.io.InputStream;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface FileUploadService {

    SourceFile uploadFile(String outputFileName, SchemaInterpretation interpretation, InputStream fileInputStream);

    Table getMetadata(String fileName);

    InputStream getImportErrorStream(String sourceFileName);
}
