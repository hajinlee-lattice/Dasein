package com.latticeengines.dellebi.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public interface FileFlowService {

    String getOutputDir(String type);

    String getErrorOutputDir();

    String getTxtDir(String type);

    String getZipDir();

    String getTargetDB(String type);

    FileType getFileType(String zipFileName);

    boolean deleteFile(String fileName);

    void registerFailedFile(String fileName);

    void initialContext(DataFlowContext context);

}
