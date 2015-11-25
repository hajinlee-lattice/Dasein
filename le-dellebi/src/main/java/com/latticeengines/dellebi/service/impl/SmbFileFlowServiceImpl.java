package com.latticeengines.dellebi.service.impl;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

import java.net.MalformedURLException;
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

@Component("smbFileFlowService")
public class SmbFileFlowServiceImpl extends BaseFileFlowService {

    static final Log log = LogFactory.getLog(SmbFileFlowServiceImpl.class);

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;

    @Value("${dellebi.smbinboxpath}")
    private String smbInboxPath;

    @Value("${dellebi.datatarget.dbname}")
    private String targetDB;

    public SmbFile getScanedFile() {

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        try {
            SmbFile smbDir = new SmbFile(smbInboxPath + "/", auth);

            SmbFile[] files = smbDir.listFiles("tgt_*");

            for (SmbFile zipFile : files) {

                if (isValidFile(zipFile)) {
                    String zipFileName = zipFile.getName();
                    log.info("Found one new file, name=" + zipFileName);
                    return zipFile;
                }
            }

        } catch (SmbException ex) {
            log.warn("Failed to get Smb file! error=" + ex.getMessage());
        } catch (MalformedURLException ex) {
            log.warn("The SMB url and auth is not correct! error=" + ex.getMessage());
        }

        return null;
    }

    @Override
    public DataFlowContext getContext() {

        DataFlowContext context = new DataFlowContext();

        SmbFile scanedFile = getScanedFile();

        if (scanedFile != null) {
            String txtFileName = null;

            String zipFileName = scanedFile.getName();
            String fileType = getFileType(zipFileName).toString();

            DellEbiExecutionLog dellEbiExecutionLog = new DellEbiExecutionLog();

            try {
                dellEbiExecutionLog.setFile(zipFileName);
                dellEbiExecutionLog.setStartDate(new Date());
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.NewFile.getStatus());
                dellEbiExecutionLogEntityMgr.createOrUpdate(dellEbiExecutionLog);
                txtFileName = downloadAndUnzip(scanedFile.getInputStream(), zipFileName);
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Downloaded.getStatus());
                dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.LOG_ENTRY, dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.TXT_FILE_NAME, txtFileName);
                context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, zipFileName);
                context.setProperty(DellEbiFlowService.FILE_SOURCE,
                        DellEbiFlowService.FILE_SOURCE_SMB);
                context.setProperty(DellEbiFlowService.FILE_TYPE, fileType);

                return context;

            } catch (Exception ex) {
                ex.printStackTrace();
                log.warn("Failed to get Smb file! error=" + ex.getMessage());
                dellEbiExecutionLogEntityMgr.recordFailure(dellEbiExecutionLog, ex.getMessage());
            }
        }

        return context;
    }

    @Override
    public boolean deleteFile(String fileName) {

        try {
            log.info("Deleting smbFile, name=" + fileName);
            NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
            SmbFile smbFile = new SmbFile(smbInboxPath + "/" + fileName, auth);
            if (smbFile.canWrite()) {
                smbFile.delete();
            }
            return true;

        } catch (Exception ex) {
            log.error("Can not delete smbFile, name=" + fileName, ex);
            return false;
        }

    }

    @Override
    public String getTargetDB() {
        return targetDB;
    }

    protected boolean isValidFile(SmbFile file) {

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

        } catch (SmbException e) {
            log.error("Can not delete smbFile, name=" + fileName, e);
            return false;
        }
        return true;
    }

    protected boolean isValidForDate(SmbFile file) {
        String fileName = file.getName();
        Long dateLong1, dateLong2;
        FileType type = getFileType(fileName);
        String typeName = type.toString();

        Date startDate = dellEbiConfigEntityMgr.getStartDate(typeName);

        if (startDate == null) {
            dateLong1 = -1L;
        } else {
            dateLong1 = startDate.getTime();
        }

        dateLong2 = file.getLastModified();

        if (dateLong2 > dateLong1) {
            return true;
        }

        return false;

    }

    protected boolean isProcessedFile(SmbFile file) {
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
