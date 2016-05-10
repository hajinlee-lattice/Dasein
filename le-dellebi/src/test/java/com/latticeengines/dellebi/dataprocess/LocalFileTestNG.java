package com.latticeengines.dellebi.dataprocess;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.functionalframework.DellEbiTestNGBase;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

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

    @Test(groups = "manual")
    public void process() {

        String typesStr = "WrongType, order_detail ,Order_Summary ,Warranty,SKU_Global,SKU_Manufacturer,"
                + "SKU_Itm_Cls_Code,Calendar,Channel,quote,Account_Cust";
        String[] typesList = typesStr.split(",");
        DataFlowContext context = dailyFlow.doDailyFlow(typesList);
        context.setProperty(DellEbiFlowService.START_TIME, System.currentTimeMillis());
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        Assert.assertEquals(result, true);
        exportAndReportService.export(context);

    }
}
