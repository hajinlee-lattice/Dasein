package com.latticeengines.dellebi.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public interface DellEbiFlowService {

    public static final String FILE_SOURCE_LOCAL = "LOCAL";
    public static final String FILE_SOURCE_SMB = "SMB";
    public static final String FILE_SOURCE = "FILE_SOURCE";
    public static final String TXT_FILE_NAME = "TXT_FILE_NAME";
    public static final String ZIP_FILE_NAME = "ZIP_FILE_NAME";

    public static final String RESULT_KEY = "RESULT";

    public static final String START_TIME = "startTime";

    DataFlowContext getFile();

    String getOutputDir(DataFlowContext context);

    void registerFailedFile(DataFlowContext context);

    boolean deleteFile(DataFlowContext context);

    FileType getFileType(DataFlowContext context);

    String getZipDir(DataFlowContext context);

    String getTxtDir(DataFlowContext context);

    String getTargetDB(DataFlowContext context);

}
