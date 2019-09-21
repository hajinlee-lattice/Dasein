package com.latticeengines.dellebi.dataprocess;

import java.io.File;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

@Deprecated
public class DailyJobDeploymentTestNG extends DellEbiTestNGBase {

    static final Logger log = LoggerFactory.getLogger(DailyJobDeploymentTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    private DellEbiExecutionLog dellEbiExecutionLog;

    @BeforeClass(groups = "deployment", enabled = false)
    public void setup() throws Exception {
        dellEbiConfigEntityMgr.initialService();
        smbClean(getValidateNameData());
        smbClean(getStartDateNameData());
    }

    @BeforeMethod(groups = "deployment", enabled = false)
    public void setUpBeforeMethod() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, dataHadoopWorkingPath);
    }

    @Test(groups = "deployment", dataProvider = "fileDataProvider", enabled = false)
    public void testExecute(String file, String sourceType, Boolean isProcessed) throws Exception {
        String fileName = getFileNameFromPath(file);
        String typesStr = "WrongType, order_detail ,Order_Summary ,Warranty,SKU_Global,SKU_Manufacturer,"
                + "SKU_Itm_Cls_Code,Calendar,Channel,quote,Account_Cust";
        String[] typesList = typesStr.split(",");
        String smbInboxPath = getSmbInboxPathByFileName(fileName);

        if (sourceType.equals("SMB")) {
            log.info("Copying file: " + file);
            smbPut(smbInboxPath, file);
        } else {
            log.info("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(file), new File(localInboxPath));
        }

        processLogEntryStatus(fileName, isProcessed);

        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        if (isProcessed == false) {
            Assert.assertEquals(result, true);
            result = exportAndReportService.export(context);
            Assert.assertEquals(result, true);
            Assert.assertEquals(HdfsUtils.fileExists(yarnConfiguration, dellEbiFlowService.getOutputDir(context)), true);
            List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, dellEbiFlowService.getTxtDir(context) + "/*.txt");
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

    @Test(groups = "deployment", dataProvider = "startDateFileDataProvider", enabled = false)
    public void testStartDate(String file, String sourceType, Boolean isSetStartDate) throws Exception {
        String fileName = getFileNameFromPath(file);
        String typesStr = "quote,order_detail";
        String[] typesList = typesStr.split(",");
        String smbInboxPath = getSmbInboxPathByFileName(fileName);

        if (sourceType.equals("SMB")) {
            log.info("Copying file: " + file);
            smbPut(smbInboxPath, file);
        } else {
            log.info("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(file), new File(localInboxPath));
        }

        processLogEntryStatus(fileName, false);

        processSmbFileModifiedDate(fileName, isSetStartDate);

        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());

        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);

        if (isSetStartDate == true) {
            Assert.assertEquals(result, false);
        } else {
            Assert.assertEquals(result, true);
            exportAndReportService.export(context);

            Assert.assertEquals(HdfsUtils.fileExists(yarnConfiguration, dellEbiFlowService.getOutputDir(context)), true);
            List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, dellEbiFlowService.getTxtDir(context) + "/*.txt");
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

    @Test(groups = "deployment", dataProvider = "filteredTypeFileDataProvider", enabled = false)
    public void testFilteredTypeFile(String file, String sourceType, Boolean isSetStartDate) throws Exception {
        String fileName = getFileNameFromPath(file);
        String typesStr = "quote,order_detail,Order_Summary,SKU_Global,SKU_Manufacturer,"
                + "SKU_Itm_Cls_Code,Calendar,Channel";
        String[] typesList = typesStr.split(",");
        String smbInboxPath = getSmbInboxPathByFileName(fileName);

        if (sourceType.equals("SMB")) {
            log.info("Copying file: " + file);
            smbPut(smbInboxPath, file);
        } else {
            log.info("Copying file: " + fileName);
            FileUtils.copyFileToDirectory(new File(file), new File(localInboxPath));
        }

        processLogEntryStatus(fileName, false);

        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());

        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);

        Assert.assertEquals(result, false);
    }

    @DataProvider(name = "fileDataProvider")
    public static Object[][] getValidateNameData() {
        return new Object[][] {

                //{ "./src/test/resources/tgt_quote_trans_global_1_2015.zip", "LOCAL", false },
                { "./src/test/resources/tgt_quote_trans_global_1_2015.zip", "SMB", false },
                { "./src/test/resources/tgt_all_account_cust_1_20160404_034341.zip", "SMB", false },
                { "./src/test/resources/fiscal_day_calendar_1_20151125_200027.zip", "SMB", false },
                { "./src/test/resources/global_sku_lookup_1_20151007_035025.zip", "SMB", false },
                { "./src/test/resources/tgt_warranty_global_1_20151129_185719.zip", "SMB", true }

        };
    }

    @DataProvider(name = "startDateFileDataProvider")
    public static Object[][] getStartDateNameData() {
        return new Object[][] { { "./src/test/resources/tgt_quote_trans_global_5_2015.zip", "SMB", true }, };
    }

    @DataProvider(name = "filteredTypeFileDataProvider")
    public static Object[][] getFilteredTypeFileNameData() {
        return new Object[][] { { "./src/test/resources/tgt_warranty_global_1_20151129_185719.zip", "SMB", true }, };
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

        return ( dellEbiConfigEntityMgr.getIsDeleted(type) == null ? false : dellEbiConfigEntityMgr.getIsDeleted(type));

    }

    private void processSmbFileModifiedDate(String fileName, Boolean isSetStartDate) throws Exception {

        if (isSetStartDate == false)
            return;

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        String smbInboxPath = getSmbInboxPathByFileName(fileName);

        SmbFile remoteFile = new SmbFile(smbInboxPath + "/" + fileName, auth);

        DataFlowContext context = new DataFlowContext();
        context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, fileName);

        String type = dellEbiFlowService.getFileType(context);

        Date date = dellEbiConfigEntityMgr.getStartDate(type);

        if (date != null) {
            Long dateLong2 = date.getTime() - 10000000;
            remoteFile.setLastModified(dateLong2);
        }
    }

    private String getSmbInboxPathByFileName(String fileName) {
        DataFlowContext context = new DataFlowContext();
        context.setProperty(DellEbiFlowService.ZIP_FILE_NAME, fileName);

        String type = dellEbiFlowService.getFileType(context);

        return dellEbiConfigEntityMgr.getInboxPath(type);

    }

    private SmbFile smbRetrieve(String remoteUrl, String localFilePath) throws Exception {
        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        String fileName = getFileNameFromPath(localFilePath);

        return new SmbFile(remoteUrl + "/" + fileName, auth);

    }
}
