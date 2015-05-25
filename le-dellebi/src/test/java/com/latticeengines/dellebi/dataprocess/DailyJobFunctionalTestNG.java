package com.latticeengines.dellebi.dataprocess;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

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
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@ContextConfiguration(locations = { "classpath:dellebi-properties-context.xml", "classpath:dellebi-context.xml" })
public class DailyJobFunctionalTestNG extends AbstractTestNGSpringContextTests {

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.smbaccount}")
    private String smbAccount;
    @Value("${dellebi.smbps}")
    private String smbPS;
    @Value("${dellebi.smbinboxpath}")
    private String smbInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @BeforeMethod(groups = "functional")
    public void setUpBeforeMethod() throws Exception {

        Configuration configuration = new Configuration();
        HdfsUtils.rmdir(configuration, dataHadoopWorkingPath);

        dellEbiFlowService.deleteSMBFile("tgt_quote_trans_global_1_2015.zip");
        dellEbiFlowService.deleteSMBFile("tgt_quote_trans_global_4_2015.zip");
    }

    @Test(groups = "functional", dataProvider = "fileDataProvider")
    public void testExecute(String fileName) throws Exception {
        smbPut(smbInboxPath, fileName);

        DataFlowContext requestContext = new DataFlowContext();
        requestContext.setProperty(DellEbiDailyJob.START_TIME, System.currentTimeMillis());

        boolean result = dailyFlow.doDailyFlow();
        Assert.assertEquals(result, true);
        exportAndReportService.export(requestContext);

        Configuration conf = new Configuration();
        Assert.assertEquals(HdfsUtils.fileExists(conf, dellEbiFlowService.getOutputDir()), true);
        List<String> files = HdfsUtils.getFilesByGlob(conf, dellEbiFlowService.getTxtDir() + "/*.txt");
        Assert.assertEquals(files.size(), 1);
    }

    @DataProvider(name = "fileDataProvider")
    public static Object[][] getValidateNameData() {
        return new Object[][] { { "./src/test/resources/tgt_quote_trans_global_1_2015.zip" },
                { "./src/test/resources/tgt_quote_trans_global_4_2015.zip" } };
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

}
