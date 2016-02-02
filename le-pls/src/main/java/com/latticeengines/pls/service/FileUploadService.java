package com.latticeengines.pls.service;

import java.io.InputStream;

import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.domain.exposed.workflow.SourceFileSchema;

public interface FileUploadService {

    SourceFile uploadFile(String outputFileName, SourceFileSchema schema, InputStream fileInputStream);
}
