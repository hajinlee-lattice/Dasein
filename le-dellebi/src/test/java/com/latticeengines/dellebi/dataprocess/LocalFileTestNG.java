package com.latticeengines.dellebi.dataprocess;

import com.latticeengines.common.exposed.util.HdfsUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

import java.io.File;

public class LocalFileTestNG extends DellEbiTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(LocalFileTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @BeforeMethod(groups = "manual")
    public void setUpBeforeMethod() throws Exception {

        Configuration configuration = new Configuration();
        HdfsUtils.rmdir(configuration, dataHadoopWorkingPath);
    }

    @Test(groups = "manual")
    public void process() {

        String typesStr = "WrongType, order_detail ,Order_Summary ,Warranty,SKU_Global,SKU_Manufacturer,"
                + "SKU_Itm_Cls_Code,Calendar,Channel,quote,Account_Cust";
        String[] typesList = typesStr.split(",");
        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        Assert.assertEquals(result, true);
        result = exportAndReportService.export(context);
        Assert.assertEquals(result, true);

    }
}
