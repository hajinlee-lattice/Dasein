package com.latticeengines.dellebi.service;


public interface FileFlowService {

    String getFile();

    String getOutputDir();
    
    String getErrorOutputDir();

    String getTxtDir();

    String getZipDir();
    
    String getTargetDB();

    FileType getFileType(String zipFileName);

    boolean deleteFile(String fileName);

    void registerFailedFile(String fileName);

}
