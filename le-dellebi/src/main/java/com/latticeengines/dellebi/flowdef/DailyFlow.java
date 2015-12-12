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

import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.MailSender;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

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
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    @Autowired
    private MailSender mailSender;

    private ArrayList<FlowDef> flowList;

    public DailyFlow(ArrayList<FlowDef> flowList) {
        this.flowList = flowList;
    }

    public DataFlowContext doDailyFlow() {

        DataFlowContext context = dellEbiFlowService.getFile();
        String fileName = context.getProperty(DellEbiFlowService.TXT_FILE_NAME, String.class);
        if (fileName == null) {
            log.info("There's no valid file or can not get file!");
            context.setProperty(DellEbiFlowService.RESULT_KEY, Boolean.FALSE);
            return context;
        }

        log.info("Found new file, name=" + fileName);
        log.info("Start Cascading job!");

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        properties.put("mapred.job.queue.name", queue);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        try {
            FlowDef flow = getFlowFromFile(context);

            log.info("Cascading starts to process file! type=" + flow.getName());
            String workOutDir = dellEbiFlowService.getOutputDir(context);
            hadoopfilesystemoperations.cleanFolder(workOutDir);
            flowConnector.connect(flow).complete();
            log.info("Cascading finished to process DellEbi file! type=" + flow.getName());

        } catch (PlannerException e) {
            log.error("Cascading failed!", e);
            mailSender.sendEmail(mailReceiveList,
                    "Dell EBI daily refresh just failed! file=" + fileName,
                    "check " + dellebiEnv + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(context, e.getMessage());
            context.setProperty(DellEbiFlowService.RESULT_KEY, false);
            return context;

        } catch (Exception e) {
            log.error("Daily flow failed!", e);
            mailSender.sendEmail(mailReceiveList,
                    "Dell EBI daily refresh just failed! file=" + fileName,
                    "check " + dellebiEnv + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(context, e.getMessage());
            context.setProperty(DellEbiFlowService.RESULT_KEY, false);
            return context;
        }

        DellEbiExecutionLog dellEbiExecutionLog = context.getProperty(DellEbiFlowService.LOG_ENTRY,
                DellEbiExecutionLog.class);
        dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Transformed.getStatus());
        dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);

        log.info("Finished Cascading job!");

        context.setProperty(DellEbiFlowService.RESULT_KEY, true);
        return context;
    }

    private FlowDef getFlowFromFile(DataFlowContext context) {
        for (FlowDef flow : flowList) {
            if (flow.getName().equals(dellEbiFlowService.getFileType(context).getType())) {
                HadoopFileSystemOperations.addClasspath(flow);
                return flow;
            }
        }

        return null;
    }

}
