package com.latticeengines.dellebi.service.impl;

import jcifs.smb.NtStatus;
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
                if (ex.getNtStatus() != NtStatus.NT_STATUS_NO_SUCH_FILE)
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

        try {
            Long lastModifiedTime = file.lastModified();
            if (file.isDirectory()) {
                return false;
            }

            if (!isActive(fileName)) {
                return false;
            }

            if (isFailedFile(fileName)) {
                return false;
            }

            if (!isValidForDate(fileName, lastModifiedTime)) {
                return false;
            }

            if (isProcessedFile(fileName)) {
                return false;
            }

        } catch (SmbException e) {
            log.error("Can not validate smbFile, name=" + fileName, e);
            return false;
        }
        return true;
    }

    private void initEntries(DataFlowContext context) {
        @SuppressWarnings("unchecked")
        List<DellEbiConfig> cfgList = context.getProperty(DellEbiFlowService.CFG_LIST, List.class);
        Collections.sort(cfgList);
    }

    private List<DellEbiConfig> getEntries(DataFlowContext context) {
        @SuppressWarnings("unchecked")
        List<DellEbiConfig> cfgList = context.getProperty(DellEbiFlowService.CFG_LIST, List.class);
        return cfgList;
    }

    protected String getSmbInboxPathByFileName(String fileName) {
        FileType type = getFileType(fileName);
        if (type == null)
            return null;
        String typeName = type.getType();

        return dellEbiConfigEntityMgr.getInboxPath(typeName);

    }

}
