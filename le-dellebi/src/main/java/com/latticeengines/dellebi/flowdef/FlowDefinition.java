package com.latticeengines.dellebi.flowdef;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

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
    @Value("${dellebi.quotefields}")
    private String quoteFields;

    private static final Log log = LogFactory.getLog(FlowDefinition.class);

    @SuppressWarnings("rawtypes")
	@Bean
    public FlowDef getOrderSumDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), cascadingInpath + "/" + orderSummary
                + dataInputFileType);
        Tap outTapFile = new Hfs(new TextDelimited(true, ","), dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + orderSummary, SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("order_summary_Pipe", orderSummaryFields);
        } catch (Exception e) {
            log.error("Failed to get order summary pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName("OrderSumDailyFlow");

        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
	@Bean
    public FlowDef getOrderDetailDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), cascadingInpath + "/" + orderDetail
                + dataInputFileType);
        Tap outTapFile = new Hfs(new TextDelimited(true, ","), dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + orderDetail, SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("order_detail_Pipe", orderDetailFields);
        } catch (Exception e) {
            log.error("Failed to get order detail pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName("OrderDetailDailyFlow");
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
	@Bean
    public FlowDef getShipDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), cascadingInpath + "/"
                + shipToAddrLattice + dataInputFileType);
        Tap outTapFile = new Hfs(new TextDelimited(true, ","), dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + shipToAddrLattice, SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("ship_to_addr_lattice_Pipe", shipToAddrFields);
        } catch (Exception e) {
            log.error("Failed to get ship to addr pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName("ShipDailyFlow");
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
	@Bean
    public FlowDef getWarrantyDailyFlow() {

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), cascadingInpath + "/"
                + warrantyGlobal + dataInputFileType);
        Tap outTapFile = new Hfs(new TextDelimited(true, ","), dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + warrantyGlobal, SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("warranty_global_Pipe", warrantyFields);
        } catch (Exception e) {
            log.error("Failed to get ship to addr pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName("WarrantyDailyFlow");
        return flowDef_fileType;
    }

    @SuppressWarnings("rawtypes")
	@Bean
    public FlowDef getQuoteDailyFlow() {

        log.info("Initial quote daily flow definition!");

        Tap inTapFile = new Hfs(new TextDelimited(true, cascadingInputDelimiter), cascadingInpath + "/" + quoteTrans
                + dataInputFileType);
        Tap outTapFile = new Hfs(new TextDelimited(false, ","), dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + quoteTrans, SinkMode.UPDATE);

        Pipe copyFilePipe = new Pipe("copy");
        Pipe filePipe = null;
        try {
            filePipe = PipeFactory.getPipe("quote_trans_Pipe", quoteFields);
        } catch (Exception e) {
            log.error("Failed to get quote data pipe!", e);
        }

        FlowDef flowDef_fileType = FlowDef.flowDef().addSource(copyFilePipe, inTapFile)
                .addTailSink(filePipe, outTapFile);
        flowDef_fileType.setName("QuoteTransDailyFlow");
        return flowDef_fileType;
    }

    public void setOrderSummaryFields(String s) {
        this.orderSummaryFields = s;
    }

    public void setOrderDetailFields(String s) {
        this.orderDetailFields = s;
    }

    public void setShipToAddrFields(String s) {
        this.shipToAddrFields = s;
    }

    public void setWarrantyFields(String s) {
        this.warrantyFields = s;
    }

    public void quoteFields(String s) {
        this.quoteFields = s;
    }

    public void setCascadingInputDelimiter(String s) {
        this.cascadingInputDelimiter = s;
    }
}
