package com.latticeengines.dellebi.functionalframework;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.FileFlowService;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

@Deprecated
@ContextConfiguration(locations = { "classpath:test-dellebi-context.xml" })
public class DellEbiTestNGBase extends AbstractTestNGSpringContextTests {

    static final Logger log = LoggerFactory.getLogger(DellEbiTestNGBase.class);
    @Value("${dellebi.smbaccount}")
    protected String smbAccount;
    @Value("${dellebi.smbps}")
    protected String smbPS;

    @Autowired
    protected DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    @Autowired
    protected DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @Autowired
    protected Configuration yarnConfiguration;

    @Resource(name = "smbFileFlowService")
    protected FileFlowService smbFileFlowService;

    public void smbUpload(Object[][] fileProviderData) throws Exception {

        try {
            for (Object[] obj : fileProviderData) {
                String qualifierFileName = (String) obj[0];
                String objFileName = getFileNameFromPath(qualifierFileName);
                String fileType = smbFileFlowService.getFileType(objFileName);
                String smbInboxPath = dellEbiConfigEntityMgr.getInboxPath(fileType);
                smbPut(smbInboxPath, qualifierFileName);
            }
        } catch (SmbException ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void smbClean(Object[][] fileProviderData) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        jcifs.Config.setProperty("jcifs.smb.client.disablePlainTextPasswords", "false");
        List<DellEbiConfig> configs = dellEbiConfigEntityMgr.getConfigs();
        List<String> cleanPaths = new ArrayList<String>();
        for (DellEbiConfig config : configs) {
            if (!cleanPaths.contains(config.getInboxPath())) {
                cleanPaths.add(config.getInboxPath());
            }
        }
        for (String cleanPath : cleanPaths) {
            try {
                SmbFile remoteFile = new SmbFile(cleanPath + '/', auth);
                SmbFile[] smbFiles = remoteFile.listFiles();
                for (SmbFile smbFile : smbFiles) {
                    String fileName = smbFile.getName();

                    for (Object[] obj : fileProviderData) {
                        String objFileName = getFileNameFromPath((String) obj[0]);
                        if (fileName.equals(objFileName)) {
                            smbFile.delete();
                            log.info("Deleting smbFile, name=" + cleanPath + "/" + fileName);
                            break;
                        }
                    }
                }

            } catch (SmbException ex) {
                log.error(ex.getMessage(), ex);
            }
        }
    }

    public void smbPut(String remoteUrl, String localFile) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        InputStream in = null;
        OutputStream out = null;
        String fileName = getFileNameFromPath(localFile);
        SmbFile remoteFile = new SmbFile(remoteUrl + "/" + fileName, auth);
        in = new BufferedInputStream(new FileInputStream(localFile));
        out = new BufferedOutputStream(new SmbFileOutputStream(remoteFile));

        FileCopyUtils.copy(in, out);

    }

    public String getFileNameFromPath(String filePath) {
        if (filePath == null)
            return null;

        File localFile = new File(filePath);
        String fileName = localFile.getName();
        return fileName;
    }

}
