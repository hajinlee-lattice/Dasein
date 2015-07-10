package com.latticeengines.dellebi.service.impl;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

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

    @Override
    public DataFlowContext getFile() {
        DataFlowContext context = new DataFlowContext();
        String txtFileName = smbFileFlowService.getFile();
        String zipFileName = null;
        if (txtFileName != null) {
            zipFileName = getZipFileName(txtFileName);
            context.setProperty(TXT_FILE_NAME, txtFileName);
            context.setProperty(ZIP_FILE_NAME, zipFileName);
            context.setProperty(FILE_SOURCE, FILE_SOURCE_SMB);
            return context;
        }

        txtFileName = localFileFlowService.getFile();
        if (txtFileName != null) {
            zipFileName = getZipFileName(txtFileName);
            context.setProperty(TXT_FILE_NAME, txtFileName);
            context.setProperty(ZIP_FILE_NAME, zipFileName);
            context.setProperty(FILE_SOURCE, FILE_SOURCE_LOCAL);
        }
        return context;
    }

    private String getZipFileName(String txtFileName) {
        String zipFileName;
        if (txtFileName.endsWith(".txt")) {
            zipFileName = StringUtils.removeEnd(txtFileName, ".txt") + ".zip";
        } else {
            zipFileName = txtFileName + ".zip";
        }
        return zipFileName;
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
}
