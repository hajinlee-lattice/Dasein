package com.latticeengines.dellebi.service.impl;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("dellEbiFlowService")
public class DellEbiFlowServiceImpl implements DellEbiFlowService {

    @Resource(name = "localFileFlowService")
    private FileFlowService localFileFlowService;

    @Resource(name = "smbFileFlowService")
    private FileFlowService smbFileFlowService;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

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
        if (isSmb(context)) {
            return smbFileFlowService.deleteFile(zipFileName);
        }
        return localFileFlowService.deleteFile(zipFileName);
    }

    @Override
    public void registerFailedFile(DataFlowContext context) {
        String zipFileName = context.getProperty(ZIP_FILE_NAME, String.class);
        if (isSmb(context)) {
            smbFileFlowService.registerFailedFile(zipFileName);
        }
        localFileFlowService.registerFailedFile(zipFileName);
    }

    @Override
    public boolean runStoredProcedure(DataFlowContext context) {
        if (isSmb(context)) {
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
}
