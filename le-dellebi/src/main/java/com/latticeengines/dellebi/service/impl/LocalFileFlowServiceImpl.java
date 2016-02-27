package com.latticeengines.dellebi.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;

@Component("localFileFlowService")
public class LocalFileFlowServiceImpl extends BaseFileFlowService {

    static final Log log = LogFactory.getLog(LocalFileFlowServiceImpl.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;
    @Value("${dellebi.local.datatarget.dbname}")
    private String localTargetDB;

    @Value("${dellebi.datatarget.stagefinal.dbname}")
    private String stageFinalTargetDB;

    private DataFlowContext context;

    public File getScanedFile() {

        try {

            File inboxFile = new File(localInboxPath);
            if (!inboxFile.exists()) {
                return null;
            }
            File[] files = inboxFile.listFiles();
            if (files == null) {
                return null;
            }

            for (File zipFile : files) {
                if (isValidFile(zipFile)) {
                    String zipFileName = zipFile.getName();
                    log.info("Found one new local file, name=" + zipFileName);

                    return zipFile;
                }
            }
        } catch (Exception ex) {
            log.warn("Failed to get local file! error=" + ex.getMessage());
        }

        return null;
    }

    @Override
    public DataFlowContext getContext() {

        context = new DataFlowContext();

        File scanedFile = getScanedFile();

        if (scanedFile != null) {
            String txtFileName = null;

            String zipFileName = scanedFile.getName();
            String fileType = getFileType(zipFileName).getType();

            DellEbiExecutionLog dellEbiExecutionLog = new DellEbiExecutionLog();

            try {
                dellEbiExecutionLog.setFile(zipFileName);
                dellEbiExecutionLog.setStartDate(new Date());
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.NewFile.getStatus());
                dellEbiExecutionLogEntityMgr.createOrUpdate(dellEbiExecutionLog);
                txtFileName = downloadAndUnzip(new FileInputStream(scanedFile), zipFileName);
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Downloaded.getStatus());
                dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.LOG_ENTRY, dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.TXT_FILE_NAME, txtFileName);
                context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, zipFileName);
                context.setProperty(DellEbiFlowService.FILE_SOURCE,
                        DellEbiFlowService.FILE_SOURCE_LOCAL);
                context.setProperty(DellEbiFlowService.FILE_TYPE, fileType);

                return context;

            } catch (Exception ex) {
                ex.printStackTrace();
                log.warn("Failed to get local file! error=" + ex.getMessage());
                dellEbiExecutionLogEntityMgr.recordFailure(dellEbiExecutionLog, ex.getMessage());
            }
        }

        return context;
    }

    @Override
    public boolean deleteFile(String fileName) {
        log.info("Deleting local File, name=" + fileName);
        return new File(localInboxPath + "/" + fileName).delete();
    }

    @Override
    public String getTargetDB() {
        String type = context.getProperty(DellEbiFlowService.FILE_TYPE, String.class);
        if (type.equals(FileType.QUOTE.getType()))
            return localTargetDB;
        else
            return stageFinalTargetDB;
    }

    protected boolean isValidFile(File file) {

        String fileName = file.getName();

        try {
            if (file.isDirectory()) {
                return false;
            }

            if (!fileName.endsWith(".zip")) {
                return false;
            }
            if (isFailedFile(fileName)) {
                return false;
            }

            if (!isValidForDate(file)) {
                return false;
            }

            if (isProcessedFile(file)) {
                return false;
            }

            for (FileType type : FileType.values()) {
                if (type.equals(getFileType(fileName))) {
                    return true;
                }
            }

        } catch (Exception e) {
            log.error("Can not validate the local file, name=" + fileName, e);
            return false;
        }
        return true;
    }

    protected boolean isValidForDate(File file) {
        String fileName = file.getName();
        Long dateLong1, dateLong2;
        FileType type = getFileType(fileName);
        if (type == null)
            return false;

        String typeName = type.getType();

        Date startDate = dellEbiConfigEntityMgr.getStartDate(typeName);

        if (startDate == null) {
            dateLong1 = -1L;
        } else {
            dateLong1 = startDate.getTime();
        }

        dateLong2 = file.lastModified();

        if (dateLong2 > dateLong1) {
            return true;
        }

        return false;

    }

    protected boolean isProcessedFile(File file) {
        String filename = file.getName();
        DellEbiExecutionLog dellEbiExecutionLog = dellEbiExecutionLogEntityMgr
                .getEntryByFile(filename);

        if (dellEbiExecutionLog == null) {
            return false;
        }

        if (dellEbiExecutionLog.getStatus() == DellEbiExecutionLogStatus.Completed.getStatus()) {
            return true;
        }

        return false;
    }

}
