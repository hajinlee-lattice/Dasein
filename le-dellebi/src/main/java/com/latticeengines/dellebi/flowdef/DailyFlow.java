package com.latticeengines.dellebi.flowdef;

import java.util.ArrayList;
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

import com.latticeengines.dellebi.service.DellEbiFlowService;
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

    @Value("${dellebi.env}")
    private String dellebiEnv;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    @Autowired
    private MailSender mailSender;

    private ArrayList<FlowDef> flowList;

    public DailyFlow(ArrayList<FlowDef> flowList) {
        this.flowList = flowList;
    }

    public boolean doDailyFlow() {

        String fileName = dellEbiFlowService.getFile();
        if (fileName == null) {
            log.info("There's no file found or can not get file!");
            return false;
        }

        log.info("Found new file, name=" + fileName);
        log.info("Start Cascading job!");

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        try {
            FlowDef flow = getFlowFromFile(fileName);

            log.info("Cascading starts to process file! type=" + flow.getName());
            String workOutDir = dellEbiFlowService.getOutputDir();
            hadoopfilesystemoperations.cleanFolder(workOutDir);
            flowConnector.connect(flow).complete();
            log.info("Cascading finished to process quote file! type=" + flow.getName());

        } catch (PlannerException e) {
            log.error("Cascading failed!", e);
            mailSender.sendEmail(mailReceiveList, "Dell EBI daily refresh just failed! ", "check " + dellebiEnv
                    + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(fileName);
            return false;

        } catch (Exception e) {
            log.error("Daily flow failed!", e);
            mailSender.sendEmail(mailReceiveList, "Dell EBI daily refresh just failed! ", "check " + dellebiEnv
                    + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(fileName);
            return false;
        }

        log.info("Finished Cascading job!");

        return true;
    }

    private FlowDef getFlowFromFile(String fileName) {
        for (FlowDef flow : flowList) {
            if (flow.getName().equals(dellEbiFlowService.getFileType(fileName).getType())) {
                return flow;
            }
        }
        return null;
    }
}
