package com.latticeengines.dellebi.service.impl;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.service.FileType;

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
    
    @Override
    public String getFile() {

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        try {
            SmbFile smbDir = new SmbFile(smbInboxPath + "/", auth);

            SmbFile[] files = smbDir.listFiles("tgt_quote_*");
            String txtFileName = null;
            for (SmbFile zipFile : files) {
                if (zipFile.isDirectory()) {
                    continue;
                }
                String zipFileName = zipFile.getName();
                if (!zipFileName.endsWith(".zip")) {
                    continue;
                }
                if (isFailedFile(zipFileName)) {
                    continue;
                }
                if (FileType.QUOTE.equals(getFileType(zipFileName))) {
                    log.info("Found one new quote file, name=" + zipFileName);
                    txtFileName = downloadAndUnzip(zipFile.getInputStream(), zipFile.getName());
                    break;
                }
            }
            return txtFileName;

        } catch (Exception ex) {
            log.warn("Failed to get Smb file! error=" + ex.getMessage());
        }

        return null;
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

}
