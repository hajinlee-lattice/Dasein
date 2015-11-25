package com.latticeengines.dellebi.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public interface FileFlowService {

    String getOutputDir();

    String getErrorOutputDir();

    String getTxtDir();

    String getZipDir();

    String getTargetDB();

    DataFlowContext getContext();

    FileType getFileType(String zipFileName);

    boolean deleteFile(String fileName);

    void registerFailedFile(String fileName);

}
