package com.latticeengines.pls.service;

import java.io.InputStream;

import com.latticeengines.domain.exposed.workflow.SourceFile;

public interface FileUploadService {

    SourceFile uploadFile(String outputFileName, InputStream fileInputStream);
}
