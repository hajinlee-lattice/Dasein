package com.latticeengines.dellebi.dataprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Deprecated
public class LocalFileTestNG extends DellEbiTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(LocalFileTestNG.class);

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.local.inboxpath}")
    private String localInboxPath;

    @Autowired
    private DailyFlow dailyFlow;

    @Autowired
    private ExportAndReportService exportAndReportService;

    @BeforeMethod(groups = "manual", enabled = false)
    public void setUpBeforeMethod() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, dataHadoopWorkingPath);
    }

    @Test(groups = "manual", enabled = false)
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
