package com.latticeengines.dellebi.flowdef;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.property.AppProps;

import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.MailSender;

public class DailyFlow {

    private static final Log log = LogFactory.getLog(DailyFlow.class);

    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInPath;
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

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

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.mailreceivelist}")
    private String mailReceiveList;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    @Autowired
    private MailSender mailSender;

    private ArrayList<FlowDef> flowList;

    public DailyFlow(ArrayList<FlowDef> flowList) {
        this.flowList = flowList;
    }

    public int doDailyFlow() {

        log.info("All daily refresh files arrive!");
        log.info("Start process data on HDFS!");

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        log.info("Start to process income files.");

        try {
            for (Iterator<FlowDef> flow = flowList.iterator(); flow.hasNext();) {
                FlowDef item = flow.next();

                if ("OrderSumDailyFlow".equals(item.getName())
                        && (hadoopfilesystemoperations.listFileNumber(dataHadoopInPath + "/" + orderSummary) != 0)) {
                    flowConnector.connect(item).complete();
                    // Remove .txt file after cascading processes data.
                    hadoopfilesystemoperations.cleanFolder(dataHadoopInPath + "/" + orderSummary);
                } else if ("OrderDetailDailyFlow".equals(item.getName())
                        && (hadoopfilesystemoperations.listFileNumber(dataHadoopInPath + "/" + orderDetail) != 0)) {
                    flowConnector.connect(item).complete();
                    hadoopfilesystemoperations.cleanFolder(dataHadoopInPath + "/" + orderDetail);
                } else if ("ShipDailyFlow".equals(item.getName())
                        && (hadoopfilesystemoperations.listFileNumber(dataHadoopInPath + "/" + shipToAddrLattice) != 0)) {
                    flowConnector.connect(item).complete();
                    hadoopfilesystemoperations.cleanFolder(dataHadoopInPath + "/" + shipToAddrLattice);
                } else if ("WarrantyDailyFlow".equals(item.getName())
                        && (hadoopfilesystemoperations.listFileNumber(dataHadoopInPath + "/" + warrantyGlobal) != 0)) {
                    flowConnector.connect(item).complete();
                    hadoopfilesystemoperations.cleanFolder(dataHadoopInPath + "/" + warrantyGlobal);

                } else if ("QuoteTransDailyFlow".equals(item.getName())
                        && hadoopfilesystemoperations.ifReadyToProcessData() == true) {
                    log.info("Cascading starts to process quote files!");
                    if (hadoopfilesystemoperations.isExist(dataHadoopWorkingPath + "/" + quoteTrans) == true) {
                        if (hadoopfilesystemoperations.isExist(dataHadoopWorkingPath + "/" + quoteTrans + "/_SUCCESS") == false) {
                            log.warn("Cascading output is not clean, cleanup the directory!");
                            hadoopfilesystemoperations.cleanFolder(dataHadoopWorkingPath + "/" + quoteTrans);
                        } else {
                            log.info("Cascading output directory has result, return!");
                            return 0;
                        }
                    }
                    flowConnector.connect(item).complete();
                    log.info("Cascading finished to process quote files!");
                    if (hadoopfilesystemoperations.isExist(dataHadoopWorkingPath + "/" + quoteTrans + "/_SUCCESS") == true) {
                        log.info("Cascading process is done.  Removing txt files!");
                        hadoopfilesystemoperations.cleanFolder(dataHadoopInPath + "/" + quoteTrans);
                    } else {
                        log.warn("Cascading failed to process data.");
                        return 3;
                    }
                }
            }
        } catch (PlannerException e) {
            log.error("Cascading failed!", e);
            mailSender.sendEmail(mailReceiveList,
                    "Dell EBI daily refresh just failed for Cascading because of some reasons!",
                    "check if there is corrupt data! on " + System.getProperty("DELLEBI_PROPDIR")
                            + " environment. error=" + e);
            return 1;
        } catch (Exception e) {
            log.error("Failed!", e);
            mailSender.sendEmail(mailReceiveList, "Dell EBI daily refresh just failed because of some reasons!",
                    "Please check Dell EBI logs on " + System.getProperty("DELLEBI_PROPDIR") + " environment. error="
                            + e);
            return 2;
        }

        return 0;
    }

    public void setDataHadoopInPath(String s) {
        this.dataHadoopInPath = s;
    }

    public void setOrderSummary(String s) {
        this.orderSummary = s;
    }

    public void setOrderDetail(String s) {
        this.orderDetail = s;
    }

    public void setShipToAddrLattice(String s) {
        this.shipToAddrLattice = s;
    }

    public void setWarrantyGlobal(String s) {
        this.warrantyGlobal = s;
    }

    public void setQuoteTrans(String s) {
        this.quoteTrans = s;
    }

    public void setDataHadoopWorkingPath(String s) {
        this.dataHadoopWorkingPath = s;
    }

    public void setMailReceiveList(String s) {
        this.mailReceiveList = s;
    }

}
