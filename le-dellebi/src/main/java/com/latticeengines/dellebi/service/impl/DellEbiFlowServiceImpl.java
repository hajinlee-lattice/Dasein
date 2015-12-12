package com.latticeengines.dellebi.service.impl;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;

@Component("dellEbiFlowService")
public class DellEbiFlowServiceImpl implements DellEbiFlowService {

    @Resource(name = "localFileFlowService")
    private FileFlowService localFileFlowService;

    @Resource(name = "smbFileFlowService")
    private FileFlowService smbFileFlowService;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @Value("${dellebi.output.table.sample}")
    private String targetTable;

    @Autowired
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    @Override
    public DataFlowContext getFile() {
        DataFlowContext context = new DataFlowContext();
        context = smbFileFlowService.getContext();
        String fileName = context.getProperty(ZIP_FILE_NAME, String.class);

        if (fileName != null) {
            return context;
        }

        context = localFileFlowService.getContext();

        return context;
    }

    private boolean isSmb(DataFlowContext context) {
        if (context == null) {
            return true;
        }
        String smb = context.getProperty(FILE_SOURCE, String.class);
        if (FILE_SOURCE_SMB.equals(smb)) {
            return true;
        }
        return false;
    }

    @Override
    public String getOutputDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getOutputDir();
        }
        return localFileFlowService.getOutputDir();
    }

    @Override
    public String getTxtDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getTxtDir();
        }
        return localFileFlowService.getTxtDir();
    }

    @Override
    public String getZipDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getZipDir();
        }
        return localFileFlowService.getZipDir();
    }

    @Override
    public FileType getFileType(DataFlowContext context) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        if (isSmb(context)) {
            return smbFileFlowService.getFileType(zipFileName);
        }
        return localFileFlowService.getFileType(zipFileName);
    }

    @Override
    public String getTargetDB(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getTargetDB();
        }
        return localFileFlowService.getTargetDB();
    }

    @Override
    public boolean deleteFile(DataFlowContext context) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        Boolean isDeleted;
        String fileType;

        fileType = getFileType(context).getType();

        isDeleted = dellEbiConfigEntityMgr.getIsDeleted(fileType);

        if (!isDeleted || isDeleted == false)
            return true;

        if (isSmb(context)) {
            return smbFileFlowService.deleteFile(zipFileName);
        }
        return localFileFlowService.deleteFile(zipFileName);
    }

    @Override
    public void registerFailedFile(DataFlowContext context, String err) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        if (isSmb(context)) {
            smbFileFlowService.registerFailedFile(zipFileName);
        }
        localFileFlowService.registerFailedFile(zipFileName);

        DellEbiExecutionLog dellEbiExecutionLog = context.getProperty(LOG_ENTRY,
                DellEbiExecutionLog.class);
        dellEbiExecutionLogEntityMgr.recordFailure(dellEbiExecutionLog, err);
    }

    @Override
    public boolean runStoredProcedure(DataFlowContext context) {

        String type = context.getProperty(FILE_TYPE, String.class);
        if (isSmb(context) && type.equals(FileType.QUOTE.getType())) {
            return true;
        }
        return false;
    }

    @Override
    public String getErrorOutputDir(DataFlowContext context) {
        if (isSmb(context)) {
            return smbFileFlowService.getErrorOutputDir();
        }
        return localFileFlowService.getErrorOutputDir();
    }

    @Override
    public String getTargetColumns(DataFlowContext context) {
        String fileType = context.getProperty(FILE_TYPE, String.class);

        return dellEbiConfigEntityMgr.getTargetColumns(fileType);
    }

    @Override
    public String getTargetTable(DataFlowContext context) {
        String fileType = context.getProperty(FILE_TYPE, String.class);

        if (fileType.equals(FileType.QUOTE.getType())) {
            return targetTable;
        }
        return dellEbiConfigEntityMgr.getTargetTable(fileType);
    }
}
