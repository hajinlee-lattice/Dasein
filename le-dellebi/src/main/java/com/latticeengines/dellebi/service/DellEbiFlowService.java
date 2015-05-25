package com.latticeengines.dellebi.service;


public interface DellEbiFlowService {

    String getFile();

    String getOutputDir();

    String getTxtDir();

    String getZipDir();

    FileType getFileType(String zipFileName);

    boolean deleteSMBFile(String fileName);

    void registerFailedFile(String fileName);

}
