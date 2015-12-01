package com.latticeengines.pls.service;

import java.io.InputStream;

public interface FileUploadService {

    void uploadFile(String outputFileName, InputStream fileInputStream);
}
