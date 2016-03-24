package com.latticeengines.dellebi.mbean;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.mbean.SmbFilesMBean;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

public class SmbFilesMBeanTestNG extends DellEbiTestNGBase {
    static final Log log = LogFactory.getLog(SmbFilesMBeanTestNG.class);

    @Autowired
    private SmbFilesMBean smbFilesMBean;

    private final static String quote_type = "quote";
    private final static String order_detail_type = "order_detail";
    private final static String channel_type = "Channel";
    private final static String[] sortedFileNames = { "tgt_quote_trans_global_1_2015.zip",
            "tgt_quote_trans_global_5_2015.zip", "tgt_quote_trans_global_1_20150107_053143.zip",
            "tgt_quote_trans_global_2_20151007_035025.zip", "tgt_quote_trans_global_11_20151007_035025.zip",
            "tgt_quote_trans_global_1_2016.zip", "tgt_quote_trans_global_1_20161007_035025.zip",
            "tgt_order_detail_global_1_20151127_235435.zip", "tgt_order_detail_global_5_20151127_235435.zip",
            "tgt_all_chnl_hier_1_20151125_201055.zip", "tgt_order_detail_global_5_20151127_235435_1.zip" };

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        dellEbiConfigEntityMgr.initialService();
        smbUpload(getUploadedFileData());
    }

    @Test(groups = "functional")
    public void testSmbFilesMBean() {

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", smbAccount, smbPS);

        SmbFile remoteFile;
        SmbFile[] smbFiles = null;
        List<SmbFile> smbFilesList = new ArrayList<SmbFile>();
        try {
            String smbInboxPath = dellEbiConfigEntityMgr.getInboxPath(quote_type);
            remoteFile = new SmbFile(smbInboxPath + '/', auth);

            smbFiles = remoteFile.listFiles(dellEbiConfigEntityMgr.getFilePattern(quote_type));

            for (int i = 0; i < smbFiles.length; i++) {
                smbFilesList.add(smbFiles[i]);
            }

            smbInboxPath = dellEbiConfigEntityMgr.getInboxPath(order_detail_type);
            remoteFile = new SmbFile(smbInboxPath + '/', auth);

            smbFiles = remoteFile.listFiles(dellEbiConfigEntityMgr.getFilePattern(order_detail_type));

            for (int i = 0; i < smbFiles.length; i++) {
                smbFilesList.add(smbFiles[i]);
            }
            smbInboxPath = dellEbiConfigEntityMgr.getInboxPath(channel_type);
            remoteFile = new SmbFile(smbInboxPath + '/', auth);

            smbFiles = remoteFile.listFiles(dellEbiConfigEntityMgr.getFilePattern(channel_type));

            for (int i = 0; i < smbFiles.length; i++) {
                smbFilesList.add(smbFiles[i]);
            }

        } catch (Exception e) {
            log.error(e);
        }
        smbFiles = smbFilesList.toArray(new SmbFile[smbFilesList.size()]);
        smbFilesMBean.sortSmbFiles(smbFiles);
        for (SmbFile file : smbFiles) {
            log.info(file.getName());
        }
        Assert.assertArrayEquals(sortedFileNames, smbFilesMBean.getSmbFileNamesArray(smbFiles));

    }

    @Test(groups = "functional")
    public void testInvalidSmbFiles() {

        String wrongName1 = "tgt_order_detail_global_5_20151127_235435_1.zip";
        String wrongName2 = "tgt_order_detail_global_5_201511271_235435.zip";
        String returnStr = null;

        returnStr = smbFilesMBean.parseSmbFileName(wrongName1);

        Assert.assertEquals(SmbFilesMBean.INVALID_PARSEDSTR, returnStr);

        returnStr = smbFilesMBean.parseSmbFileName(wrongName2);

        Assert.assertEquals(SmbFilesMBean.INVALID_PARSEDSTR, returnStr);

        smbFilesMBean.sortSmbFiles(null);
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        smbClean(getUploadedFileData());
    }

    @DataProvider(name = "fileDataProvider")
    public static Object[][] getUploadedFileData() {
        return new Object[][] { { "./src/test/resources/tgt_order_detail_global_5_20151127_235435_1.zip" },
                { "./src/test/resources/tgt_quote_trans_global_1_2015.zip" },
                { "./src/test/resources/tgt_quote_trans_global_5_2015.zip" },
                { "./src/test/resources/tgt_quote_trans_global_1_20161007_035025.zip" },
                { "./src/test/resources/tgt_quote_trans_global_1_2016.zip" },
                { "./src/test/resources/tgt_quote_trans_global_2_20151007_035025.zip" },
                { "./src/test/resources/tgt_quote_trans_global_11_20151007_035025.zip" },
                { "./src/test/resources/tgt_order_detail_global_5_20151127_235435.zip" },
                { "./src/test/resources/tgt_quote_trans_global_1_20150107_053143.zip" },
                { "./src/test/resources/tgt_all_chnl_hier_1_20151125_201055.zip" },
                { "./src/test/resources/tgt_order_detail_global_1_20151127_235435.zip" } };
    }
}
