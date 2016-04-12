package com.latticeengines.dellebi.dataprocess;

import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class LocalFileTestNG extends DellEbiTestNGBase {

    private static final Log log = LogFactory.getLog(LocalFileTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @Test(groups = "manual")
    public void process() {

        String typesStr = "order_detail";
        String[] typesList = typesStr.split(",");
        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        Assert.assertEquals(result, true);
        exportAndReportService.export(context);

    }
}
