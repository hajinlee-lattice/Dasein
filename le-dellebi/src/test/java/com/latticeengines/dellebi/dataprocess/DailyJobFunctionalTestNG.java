package com.latticeengines.dellebi.dataprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
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

    static final Log log = LogFactory.getLog(DailyJobFunctionalTestNG.class);

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

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    private DellEbiExecutionLog dellEbiExecutionLog;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        smbClean(smbInboxPath);
    }

    @BeforeMethod(groups = "functional")
    public void setUpBeforeMethod() throws Exception {

        Configuration configuration = new Configuration();
        HdfsUtils.rmdir(configuration, dataHadoopWorkingPath);

        FileUtils.deleteDirectory(new File(localInboxPath));
        FileUtils.forceMkdir(new File(localInboxPath));
    }

    @Test(groups = "functional", dataProvider = "fileDataProvider")
    public void testExecute(String file, String sourceType, Boolean isProcessed) throws Exception {
        String fileName = getFileNameFromPath(file);

        if (sourceType.equals("SMB")) {
            log.info("Copying file: " + file);
            smbPut(smbInboxPath, file);
        } else {
            log.info("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(file), new File(localInboxPath));
        }

        processLogEntryStatus(fileName, isProcessed);

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
            if (sourceType.equals("SMB")) {
                SmbFile smbFile = smbRetrieve(smbInboxPath, file);
                if (getIsDeleted(context)) {
                    Assert.assertEquals(smbFile.exists(), false);
                } else if (!getIsDeleted(context)) {
                    Assert.assertEquals(smbFile.exists(), true);
                }
            }
        } else {
            Assert.assertEquals(result, false);
        }

    }

    @Test(groups = "functional", dataProvider = "startDateFileDataProvider")
    public void testStartDate(String file, String sourceType, Boolean isSetStartDate)
            throws Exception {
        String fileName = getFileNameFromPath(file);

        if (sourceType.equals("SMB")) {
            log.info("Copying file: " + file);
            smbPut(smbInboxPath, file);
        } else {
            log.info("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(file), new File(localInboxPath));
        }

        processLogEntryStatus(fileName, false);

        processSmbFileModifiedDate(fileName, isSetStartDate);

        DataFlowContext context = dailyFlow.doDailyFlow();
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());

        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);

        if (isSetStartDate == true) {
            Assert.assertEquals(result, false);
        } else {
            Assert.assertEquals(result, true);
            exportAndReportService.export(context);

            Configuration conf = new Configuration();
            Assert.assertEquals(HdfsUtils.fileExists(conf, dellEbiFlowService.getOutputDir(null)),
                    true);
            List<String> files = HdfsUtils.getFilesByGlob(conf,
                    dellEbiFlowService.getTxtDir(null) + "/*.txt");
            Assert.assertEquals(files.size(), 1);
            if (sourceType.equals("SMB")) {
                SmbFile smbFile = smbRetrieve(smbInboxPath, file);
                if (getIsDeleted(context)) {
                    Assert.assertEquals(smbFile.exists(), false);
                } else if (!getIsDeleted(context)) {
                    Assert.assertEquals(smbFile.exists(), true);
                }
            }
        }
    }

    @DataProvider(name = "fileDataProvider")
    public static Object[][] getValidateNameData() {
        return new Object[][] {

                { "./src/test/resources/tgt_quote_trans_global_1_2015.zip", "LOCAL", false },
                { "./src/test/resources/fiscal_day_calendar_1_20151125_200027.zip", "SMB", false },
                { "./src/test/resources/global_sku_lookup_1_20151007_035025.zip", "SMB", false },
                { "./src/test/resources/tgt_all_chnl_hier_1_20151125_201055.zip", "SMB", false },
                { "./src/test/resources/tgt_itm_cls_code_lattice_ext_1_20151129_020123.zip", "SMB",
                        false },
                { "./src/test/resources/tgt_lat_order_summary_global_1_20151126_201516.zip", "SMB",
                        false },
                { "./src/test/resources/tgt_lattice_mfg_ext_1_20151109_020120.zip", "SMB", false },
                { "./src/test/resources/tgt_order_detail_global_1_20151127_235435.zip", "SMB",
                        false },
                { "./src/test/resources/tgt_warranty_global_1_20151129_185719.zip", "SMB", false },
                { "./src/test/resources/tgt_warranty_global_1_20151129_185719.zip", "SMB", true }

        };
    }

    @DataProvider(name = "startDateFileDataProvider")
    public static Object[][] getStartDateNameData() {
        return new Object[][] {
                { "./src/test/resources/tgt_quote_trans_global_5_2015.zip", "SMB", true }, };
    }

    private void smbPut(String remoteUrl, String localFile) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        InputStream in = null;
        OutputStream out = null;
        String fileName = getFileNameFromPath(localFile);
        SmbFile remoteFile = new SmbFile(remoteUrl + "/" + fileName, auth);
        in = new BufferedInputStream(new FileInputStream(localFile));
        out = new BufferedOutputStream(new SmbFileOutputStream(remoteFile));

        FileCopyUtils.copy(in, out);

    }

    private void smbClean(String remoteUrl) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);
        jcifs.Config.setProperty("jcifs.smb.client.disablePlainTextPasswords","false");
        try {
            SmbFile remoteFile = new SmbFile(remoteUrl + '/', auth);
            SmbFile[] smbFiles = remoteFile.listFiles();
            for (SmbFile smbFile : smbFiles) {
                String fileName = smbFile.getName();

                for (Object[] obj : getValidateNameData()) {
                    String objFileName = getFileNameFromPath((String) obj[0]);
                    if (fileName.equals(objFileName)) {
                        smbFile.delete();
                        log.info("Deleting smbFile, name=" + remoteUrl + "/" + fileName);
                        break;
                    }
                }

                for (Object[] obj : getStartDateNameData()) {
                    String objFileName = getFileNameFromPath((String) obj[0]);
                    if (fileName.equals(objFileName)) {
                        smbFile.delete();
                        log.info("Deleting smbFile, name=" + remoteUrl + "/" + fileName);
                        break;
                    }
                }

            }

        } catch (SmbException ex) {
            ex.printStackTrace();
            return;
        }
    }

    private SmbFile smbRetrieve(String remoteUrl, String localFilePath) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        String fileName = getFileNameFromPath(localFilePath);

        return new SmbFile(remoteUrl + "/" + fileName, auth);

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

    private Boolean getIsDeleted(DataFlowContext context) {

        String type = dellEbiFlowService.getFileType(context).toString();

        return dellEbiConfigEntityMgr.getIsDeleted(type);

    }

    private String getFileNameFromPath(String filePath) {
        if (filePath == null)
            return null;

        File localFile = new File(filePath);
        String fileName = localFile.getName();
        return fileName;
    }

    private void processSmbFileModifiedDate(String fileName, Boolean isSetStartDate)
            throws Exception {

        if (isSetStartDate == false)
            return;

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        SmbFile remoteFile = new SmbFile(smbInboxPath + "/" + fileName, auth);

        DataFlowContext context = new DataFlowContext();
        context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, fileName);

        String type = dellEbiFlowService.getFileType(context).getType();

        Date date = dellEbiConfigEntityMgr.getStartDate(type);

        if (date != null) {
            Long dateLong2 = date.getTime() - 10000000;
            remoteFile.setLastModified(dateLong2);
        }
    }
}
