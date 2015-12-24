package com.latticeengines.dellebi.flowdef;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.service.FileType;
import com.latticeengines.dellebi.util.PipeFactory;

@Configuration
public class FlowDefinition {

    @Value("${dellebi.cascadinginpath}")
    private String cascadingInpath;
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.datainputfiletype}")
    private String dataInputFileType;

    @Value("${dellebi.cascadinginputdelimiter}")
    private String cascadingInputDelimiter;

    @Value("${dellebi.ordersummary}")
    private String orderSummary;
    @Value("${dellebi.orderdetail}")
    private String orderDetail;
    @Value("${dellebi.shiptoaddrlattice}")
    private String shipToAddrLattice;
    @Value("${dellebi.warrantyglobal}")
    private String warrantyGlobal;
    @Value("${dellebi.quotetrans}")
    private String quoteTrans;

    @Value("${dellebi.orderdetailfields}")
    private String orderDetailFields;
    @Value("${dellebi.shiptoaddrfields}")
    private String shipToAddrFields;
    @Value("${dellebi.warrantyfields}")
    private String warrantyFields;

    @Value("${dellebi.exportedorderdetailfields}")
    private String exportedOrderDetailFields;
    @Value("${dellebi.exportedshiptoaddrfields}")
    private String exportedShipToAddrFields;
    @Value("${dellebi.exportedwarrantyfields}")
    private String exportedWarrantyFields;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    private static final Log log = LogFactory.getLog(FlowDefinition.class);

    @Bean
    public FlowDef initialConfigs() {
        dellEbiConfigEntityMgr.initialService();
        return null;
    }

    @Bean
    public FlowDef getOrderSumDailyFlow() {
        return getGenericItemDailyFlow(FileType.ORDER_SUMMARY.getType());
    }

    @Bean
    public FlowDef getOrderDetailDailyFlow() {
        return getGenericItemDailyFlow(FileType.ORDER_DETAIL.getType());
    }

    @Bean
    public FlowDef getQuoteDailyFlow() {
        return getGenericItemDailyFlow(FileType.QUOTE.getType());
    }

    @Bean
    public FlowDef getSkuItemClassDailyFlow() {
        return getGenericItemDailyFlow(FileType.SKU_ITM_CLS_CODE.getType());
    }

    @Bean
    public FlowDef getSkuMfgDailyFlow() {
        return getGenericItemDailyFlow(FileType.SKU_MANUFACTURER.getType());
    }

    @Bean
    public FlowDef getSkuGlobalDailyFlow() {
        return getGenericItemDailyFlow(FileType.SKU_GLOBAL.getType());
    }

    @Bean
    public FlowDef getWarrantyDailyFlow() {
        return getGenericItemDailyFlow(FileType.WARRANTY.getType());
    }

    @Bean
    public FlowDef getCalendarDailyFlow() {
        return getGenericItemDailyFlow(FileType.CALENDAR.getType());
    }

    @Bean
    public FlowDef getChannelDailyFlow() {
        return getGenericItemDailyFlow(FileType.CHANNEL.getType());
    }

    @SuppressWarnings("rawtypes")
    public FlowDef getGenericItemDailyFlow(String type) {

        log.info("Initial " + type + " flow definition!");

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(false, "\t"), dellEbiFlowService.getOutputDir(null),
                SinkMode.UPDATE);
        Tap failedRowsTapFile = new Hfs(new TextDelimited(false, "\t"), dellEbiFlowService.getErrorOutputDir(null),
                SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;

        String inputFields = dellEbiConfigEntityMgr.getInputFields(type);
        String exportedFields = dellEbiConfigEntityMgr.getOutputFields(type);

        try {
            filePipe = PipeFactory.getPipe("generic_item_Pipe", inputFields, exportedFields);
        } catch (Exception e) {
            log.error("Failed to get " + type + "  pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile).addTailSink(filePipe,
                outTapFile);
        flowDef_fileType.addTrap(filePipe, failedRowsTapFile);
        flowDef_fileType.setName(type);

        return flowDef_fileType;
    }
}
