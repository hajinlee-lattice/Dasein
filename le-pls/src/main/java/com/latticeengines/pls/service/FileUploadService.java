package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.metadata.exposed.resolution.ColumnTypeMapping;

public interface FileUploadService {

    SourceFile uploadFile(String outputFileName, SchemaInterpretation interpretation, InputStream fileInputStream);

    List<ColumnTypeMapping> getUnknownColumns(String sourceFileName);

    void resolveMetadata(String sourceFileName, List<ColumnTypeMapping> unknownColumns);

    InputStream getImportErrorStream(String sourceFileName);
}
