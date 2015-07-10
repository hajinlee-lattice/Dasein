package com.latticeengines.dellebi.service.impl;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.service.FileType;

@Component("localFileFlowService")
public class LocalFileFlowServiceImpl extends BaseFileFlowService {

    static final Log log = LogFactory.getLog(LocalFileFlowServiceImpl.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;
    @Value("${dellebi.local.datatarget.dbname}")
    private String localTargetDB;

    @Override
    public String getFile() {

        try {

            File inboxFile = new File(localInboxPath);
            if (!inboxFile.exists()) {
                return null;
            }
            File[] files = inboxFile.listFiles();
            if (files == null) {
                return null;
            }
            String txtFileName = null;
            for (File file : files) {
                if (file.isDirectory()) {
                    continue;
                }
                String zipFileName = file.getName();
                if (!zipFileName.endsWith(".zip")) {
                    continue;
                }
                if (isFailedFile(zipFileName)) {
                    continue;
                }
                if (FileType.QUOTE.equals(getFileType(zipFileName))) {
                    log.info("Found one new quote file, name=" + zipFileName);
                    txtFileName = downloadAndUnzip(new FileInputStream(file), zipFileName);
                    break;
                }
            }
            return txtFileName;

        } catch (Exception ex) {
            log.error("Failed to get Smb file!", ex);
        }

        return null;
    }

    @Override
    public boolean deleteFile(String fileName) {
        log.info("Deleting local File, name=" + fileName);
        return new File(localInboxPath + "/" + fileName).delete();
    }

    @Override
    public String getTargetDB() {
        return localTargetDB;
    }
    
    
}
