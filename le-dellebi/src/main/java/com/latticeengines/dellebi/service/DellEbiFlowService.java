package com.latticeengines.dellebi.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public interface DellEbiFlowService {

    String FILE_SOURCE_LOCAL = "LOCAL";
    String FILE_SOURCE_SMB = "SMB";
    String FILE_SOURCE = "FILE_SOURCE";
    String TXT_FILE_NAME = "TXT_FILE_NAME";
    String ZIP_FILE_NAME = "ZIP_FILE_NAME";
    String FILE_TYPE = "FILE_TYPE";
    String LOG_ENTRY = "LOG_ENTRY";
    String CFG_LIST = "CFG_LIST";
    String TYPES_LIST = "TYPES_LIST";
    String RESULT_KEY = "RESULT";
    String NO_FILE_FOUND = "NO_FILE_FOUND";

    String START_TIME = "startTime";

    DataFlowContext getFile(DataFlowContext context);

    String getOutputDir(DataFlowContext context);

    String getErrorOutputDir(DataFlowContext context);

    boolean deleteFile(DataFlowContext context);

    String getFileType(DataFlowContext context);

    String getZipDir(DataFlowContext context);

    String getTxtDir(DataFlowContext context);

    String getTargetDB(DataFlowContext context);

    String getTargetColumns(DataFlowContext context);

    boolean runStoredProcedure(DataFlowContext context);

    String getTargetTable(DataFlowContext context);

    void registerFailedFile(DataFlowContext context, String err);

}
