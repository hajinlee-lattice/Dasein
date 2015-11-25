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

    @Value("${dellebi.ordersummaryfields}")
    private String orderSummaryFields;
    @Value("${dellebi.orderdetailfields}")
    private String orderDetailFields;
    @Value("${dellebi.shiptoaddrfields}")
    private String shipToAddrFields;
    @Value("${dellebi.warrantyfields}")
    private String warrantyFields;
    @Value("${dellebi.exportedordersummaryfields}")
    private String exportedOrderSummaryFields;
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

    @SuppressWarnings("rawtypes")
    @Bean
    public FlowDef getOrderSumDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(true, ","),
                dellEbiFlowService.getOutputDir(null), SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("order_summary_Pipe", orderSummaryFields,
                    exportedOrderSummaryFields);
        } catch (Exception e) {
            log.error("Failed to get order summary pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName(FileType.ORDER_SUMMARY.getType());

        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
    @Bean
    public FlowDef getOrderDetailDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(true, ","),
                dellEbiFlowService.getOutputDir(null), SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("order_detail_Pipe", orderDetailFields,
                    exportedOrderDetailFields);
        } catch (Exception e) {
            log.error("Failed to get order detail pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName(FileType.ORDER_DETAIL.getType());
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
    @Bean
    public FlowDef getShipDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(true, ","),
                dellEbiFlowService.getOutputDir(null), SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("ship_to_addr_lattice_Pipe", shipToAddrFields,
                    exportedShipToAddrFields);
        } catch (Exception e) {
            log.error("Failed to get ship to addr pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName(FileType.SHIP.getType());
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
    @Bean
    public FlowDef getWarrantyDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(true, ","),
                dellEbiFlowService.getOutputDir(null), SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("warranty_global_Pipe", warrantyFields,
                    exportedWarrantyFields);
        } catch (Exception e) {
            log.error("Failed to get ship to addr pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName(FileType.WARRANTE.getType());
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
    @Bean
    public FlowDef getQuoteDailyFlow() {

        log.info("Initial quote daily flow definition!");

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter),
                dellEbiFlowService.getTxtDir(null));
        Tap outTapFile = new Hfs(new TextDelimited(false, ","),
                dellEbiFlowService.getOutputDir(null), SinkMode.UPDATE);
        Tap failedRowsTapFile = new Hfs(new TextDelimited(false, ","),
                dellEbiFlowService.getErrorOutputDir(null), SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;

        String quoteFields = dellEbiConfigEntityMgr
                .getInputFields(DellEbiConfigEntityMgr.DellEbi_Quote);
        String exportedQuoteFields = dellEbiConfigEntityMgr
                .getOutputFields(DellEbiConfigEntityMgr.DellEbi_Quote);

        try {
            filePipe = PipeFactory.getPipe("quote_trans_Pipe", quoteFields, exportedQuoteFields);
        } catch (Exception e) {
            log.error("Failed to get quote data pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.addTrap(filePipe, failedRowsTapFile);
        flowDef_fileType.setName(FileType.QUOTE.getType());
        return flowDef_fileType;
    }
}
