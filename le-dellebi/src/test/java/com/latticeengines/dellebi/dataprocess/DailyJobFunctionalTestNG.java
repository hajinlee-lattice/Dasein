package com.latticeengines.dellebi.dataprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;

@ContextConfiguration(locations = { "classpath:dellebi-properties-context.xml",
        "classpath:dellebi-context.xml" })
public class DailyJobFunctionalTestNG extends AbstractTestNGSpringContextTests {

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;
    @Value("${dellebi.smbinboxpath}")
    private String smbInboxPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    private DellEbiExecutionLog dellEbiExecutionLog;

    @BeforeMethod(groups = "functional")
    public void setUpBeforeMethod() throws Exception {

        Configuration configuration = new Configuration();
        HdfsUtils.rmdir(configuration, dataHadoopWorkingPath);

        DataFlowContext context = new DataFlowContext();
        context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, "tgt_quote_trans_global_1_2015.zip");
        dellEbiFlowService.deleteFile(context);
        context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, "tgt_quote_trans_global_5_2015.zip");
        dellEbiFlowService.deleteFile(context);

        FileUtils.deleteDirectory(new File(localInboxPath));
        FileUtils.forceMkdir(new File(localInboxPath));
        smbClean(smbInboxPath, "tgt_quote_trans_global_1_2015.zip");
        smbClean(smbInboxPath, "tgt_quote_trans_global_5_2015.zip");
    }

    @Test(groups = "functional", dataProvider = "fileDataProvider")
    public void testExecute(String fileName, String sourceType, Boolean isProcessed)
            throws Exception {
        if (sourceType.equals("SMB")) {
            System.out.println("Copying file: " + fileName);
            smbPut(smbInboxPath, fileName);
        } else {
            System.out.println("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(fileName), new File(localInboxPath));
        }

        processLogEntryStatus(getFileNameFromPath(fileName), isProcessed);

        DataFlowContext context = dailyFlow.doDailyFlow();
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        if (isProcessed == false) {
            Assert.assertEquals(result, true);
            exportAndReportService.export(context);

            Configuration conf = new Configuration();
            Assert.assertEquals(HdfsUtils.fileExists(conf, dellEbiFlowService.getOutputDir(null)),
                    true);
            List<String> files = HdfsUtils.getFilesByGlob(conf,
                    dellEbiFlowService.getTxtDir(null) + "/*.txt");
            Assert.assertEquals(files.size(), 1);
        } else {
            Assert.assertEquals(result, false);
        }
    }

    @DataProvider(name = "fileDataProvider")
    public static Object[][] getValidateNameData() {
        return new Object[][] {
                { "./src/test/resources/tgt_quote_trans_global_1_2015.zip", "SMB", false },
                { "./src/test/resources/tgt_quote_trans_global_5_2015.zip", "SMB", true },
                { "./src/test/resources/tgt_quote_trans_global_1_2015.zip", "LOCAL", false },
                { "./src/test/resources/tgt_quote_trans_global_5_2015.zip", "LOCAL", true } };
    }

    private void smbPut(String remoteUrl, String localFilePath) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        InputStream in = null;
        OutputStream out = null;
        File localFile = new File(localFilePath);
        String fileName = localFile.getName();
        SmbFile remoteFile = new SmbFile(remoteUrl + "/" + fileName, auth);
        in = new BufferedInputStream(new FileInputStream(localFile));
        out = new BufferedOutputStream(new SmbFileOutputStream(remoteFile));

        FileCopyUtils.copy(in, out);
    }

    private void smbClean(String remoteUrl, String localFilePath) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        File localFile = new File(localFilePath);
        String fileName = localFile.getName();
        try {
            SmbFile remoteFile = new SmbFile(remoteUrl + "/" + fileName, auth);
            remoteFile.delete();
        } catch (SmbException ex) {
            return;
        }
        System.out.println("Deleting smbFile, name=" + remoteUrl + "/" + fileName);
    }

    private void processLogEntryStatus(String fileName, Boolean isProcessed) {
        if (isProcessed == false) {
            dellEbiExecutionLog = dellEbiExecutionLogEntityMgr.getEntryByFile(fileName);
            if (dellEbiExecutionLog != null) {
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Failed.getStatus());
                dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
            }
        } else {
            dellEbiExecutionLog = dellEbiExecutionLogEntityMgr.getEntryByFile(fileName);
            if (dellEbiExecutionLog == null) {
                dellEbiExecutionLog = new DellEbiExecutionLog();
                dellEbiExecutionLog.setFile(fileName);
                Date endTime = new Date();
                Date startTime = DateUtils.addHours(endTime, -1);

                dellEbiExecutionLog.setStartDate(startTime);
                dellEbiExecutionLog.setEndDate(endTime);
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Completed.getStatus());
                dellEbiExecutionLogEntityMgr.createOrUpdate(dellEbiExecutionLog);
            } else {
                dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Completed.getStatus());
                dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
            }
        }
    }

    private String getFileNameFromPath(String path) {
        return (path.substring(path.lastIndexOf('/') + 1));
    }

}
