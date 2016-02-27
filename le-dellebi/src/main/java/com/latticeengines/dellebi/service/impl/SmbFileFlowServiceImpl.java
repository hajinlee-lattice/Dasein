package com.latticeengines.dellebi.service.impl;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("smbFileFlowService")
public class SmbFileFlowServiceImpl extends BaseFileFlowService {

    static final Log log = LogFactory.getLog(SmbFileFlowServiceImpl.class);

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;

    @Value("${dellebi.datatarget.dbname}")
    private String quoteTargetDB;

    @Value("${dellebi.datatarget.stagefinal.dbname}")
    private String stageFinalTargetDB;

    public SmbFile getScanedFile(DataFlowContext context) {

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        jcifs.Config.setProperty("jcifs.smb.client.disablePlainTextPasswords", "false");

        initEntries(context);

        for (DellEbiConfig config : getEntries(context)) {
            try {

                String smbInboxPath = config.getInboxPath();
                SmbFile smbDir = new SmbFile(smbInboxPath + "/", auth);

                SmbFile[] files = smbDir.listFiles(config.getFilePattern());

                for (SmbFile zipFile : files) {

                    if (isValidFile(zipFile, context)) {
                        String zipFileName = zipFile.getName();
                        log.info("Found one new file, name=" + zipFileName);
                        return zipFile;
                    }
                }

            } catch (SmbException ex) {
                if (ex.getNtStatus() != SmbException.NT_STATUS_NO_SUCH_FILE)
                    log.warn("Failed to get Smb file! error=" + ex.getMessage());
            } catch (MalformedURLException ex) {
                log.warn("The SMB url and auth is not correct! error=" + ex.getMessage());
            }
        }

        return null;
    }

    @Override
    public void initialContext(DataFlowContext context) {

        SmbFile scanedFile = getScanedFile(context);

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
                context.setProperty(DellEbiFlowService.FILE_TYPE, fileType);
                txtFileName = downloadAndUnzip(scanedFile.getInputStream(), zipFileName);
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Downloaded.getStatus());
                dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.LOG_ENTRY, dellEbiExecutionLog);
                context.setProperty(DellEbiFlowService.TXT_FILE_NAME, txtFileName);
                context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, zipFileName);
                context.setProperty(DellEbiFlowService.FILE_SOURCE, DellEbiFlowService.FILE_SOURCE_SMB);
            } catch (Exception ex) {
                dellEbiExecutionLogEntityMgr.recordFailure(dellEbiExecutionLog, ex.getMessage());
            }
        }

        return;
    }

    @Override
    public boolean deleteFile(String fileName) {

        try {
            NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
            String smbInboxPath = getSmbInboxPathByFileName(fileName);
            SmbFile smbFileFrom = new SmbFile(smbInboxPath + "/" + fileName, auth);
            SmbFile smbFileTo = new SmbFile(smbInboxPath + "/Archive/" + fileName, auth);
            log.info("Moving smbFile, from " + smbFileFrom + " to " + smbFileTo);
            if (smbFileFrom.canWrite()) {
                if (smbFileTo.exists()) {
                    smbFileTo.delete();
                }
                smbFileFrom.renameTo(smbFileTo);
            }
            return true;

        } catch (Exception ex) {
            log.error("Can not move smbFile, name=" + fileName, ex);
            return false;
        }

    }

    @Override
    public String getTargetDB(String type) {
        if (type.equals(FileType.QUOTE.getType()))
            return quoteTargetDB;
        else
            return stageFinalTargetDB;
    }

    protected boolean isValidFile(SmbFile file, DataFlowContext context) {

        String fileName = file.getName();

        String[] typesList = context.getProperty(DellEbiFlowService.TYPES_LIST, String[].class);

        try {
            if (file.isDirectory()) {
                return false;
            }

            if (!isActive(file)) {
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

            for (String type : typesList) {
                if (type.equalsIgnoreCase(getFileType(fileName).getType())) {
                    return true;
                }
            }

        } catch (SmbException e) {
            log.error("Can not validate smbFile, name=" + fileName, e);
            return false;
        }
        return true;
    }

    protected boolean isValidForDate(SmbFile file) {
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

        dateLong2 = file.getLastModified();

        if (dateLong2 > dateLong1) {
            return true;
        }

        return false;

    }

    protected boolean isActive(SmbFile file) {
        String fileName = file.getName();
        FileType type = getFileType(fileName);
        if (type == null)
            return false;
        String typeName = type.getType();

        Boolean isActive = dellEbiConfigEntityMgr.getIsActive(typeName);

        if (isActive == null) {
            throw new LedpException(LedpCode.LEDP_29001);
        }

        return isActive;

    }

    protected boolean isProcessedFile(SmbFile file) {
        String filename = file.getName();
        DellEbiExecutionLog dellEbiExecutionLog = dellEbiExecutionLogEntityMgr.getEntryByFile(filename);

        if (dellEbiExecutionLog == null) {
            return false;
        }

        if (dellEbiExecutionLog.getStatus() == DellEbiExecutionLogStatus.Completed.getStatus()) {
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private void initEntries(DataFlowContext context) {
        Collections.sort(context.getProperty(DellEbiFlowService.CFG_LIST, List.class));
    }

    @SuppressWarnings("unchecked")
    private List<DellEbiConfig> getEntries(DataFlowContext context) {
        return context.getProperty(DellEbiFlowService.CFG_LIST, List.class);
    }


    protected String getSmbInboxPathByFileName(String fileName) {
        FileType type = getFileType(fileName);
        if (type == null)
            return null;
        String typeName = type.getType();

        return dellEbiConfigEntityMgr.getInboxPath(typeName);

    }

}
