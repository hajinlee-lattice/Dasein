package com.latticeengines.dellebi.flowdef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.HadoopFileSystemOperations;
import com.latticeengines.dellebi.util.LoggingUtils;
import com.latticeengines.dellebi.util.MailSender;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.property.AppProps;

public class DailyFlow {

    private static final Logger log = LoggerFactory.getLogger(DailyFlow.class);

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

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    @Autowired
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    @Autowired
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private MailSender mailSender;

    @Autowired
    private FlowDefinition flowDefinition;

    @Autowired
    private Configuration yarnConfiguration;

    public DataFlowContext doDailyFlow(String[] typesList) {

        DataFlowContext context = new DataFlowContext();

        context.setProperty(DellEbiFlowService.TYPES_LIST, typesList);

        log.info("Processed types are:" + Arrays.asList(typesList).toString());

        dellEbiFlowService.getFile(context);

        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        if (!result) {
            log.error("Ran into an unexpected error!");
            return context;
        }

        String fileName = context.getProperty(DellEbiFlowService.TXT_FILE_NAME, String.class);
        if (fileName == null) {
            log.info("There's no valid file or can not get file!");
            context.setProperty(DellEbiFlowService.RESULT_KEY, Boolean.FALSE);
            context.setProperty(DellEbiFlowService.NO_FILE_FOUND, Boolean.TRUE);
            return context;
        }

        log.info("Found new file, name=" + fileName);
        log.info("Start Cascading job!");
        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : yarnConfiguration) {
            properties.put(entry.getKey(), entry.getValue());
        }
        AppProps.setApplicationJarClass(properties, DailyFlow.class);
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String translatedQueue = LedpQueueAssigner.overwriteQueueAssignment(queue, yarnQueueScheme);
        properties.put("mapred.job.queue.name", translatedQueue);

        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        try {
            FlowDef flow = getFlowFromConfigs(context);

            log.info("Cascading starts to process file! type=" + flow.getName());
            String workOutDir = dellEbiFlowService.getOutputDir(context);
            hadoopfilesystemoperations.cleanFolder(workOutDir);
            flowConnector.connect(flow).complete();
            log.info("Cascading finished to process DellEbi file! type=" + flow.getName());

        } catch (PlannerException e) {
            log.error("Cascading failed!", e);
            mailSender.sendEmail(mailReceiveList, "Dell EBI daily refresh just failed! file=" + fileName,
                    "check " + dellebiEnv + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(context, e.getMessage());
            context.setProperty(DellEbiFlowService.RESULT_KEY, false);
            return context;

        } catch (Exception e) {
            log.error("Daily flow failed!", e);
            mailSender.sendEmail(mailReceiveList, "Dell EBI daily refresh just failed! file=" + fileName,
                    "check " + dellebiEnv + " environment. error=" + e);
            dellEbiFlowService.registerFailedFile(context, e.getMessage());
            context.setProperty(DellEbiFlowService.RESULT_KEY, false);
            return context;
        }

        DellEbiExecutionLog dellEbiExecutionLog = context.getProperty(DellEbiFlowService.LOG_ENTRY,
                DellEbiExecutionLog.class);
        dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Transformed.getStatus());
        dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);

        LoggingUtils.logInfoWithDuration(log, dellEbiExecutionLog, "Finish Cascading job!", startTime);

        context.setProperty(DellEbiFlowService.RESULT_KEY, true);
        return context;
    }

    private FlowDef getFlowFromConfigs(DataFlowContext context) {

        List<DellEbiConfig> configsList = dellEbiConfigEntityMgr.getConfigs();
        for (DellEbiConfig config : configsList) {
            if (config.getType().equalsIgnoreCase(dellEbiFlowService.getFileType(context))) {

                FlowDef flow = flowDefinition.populateFlowDefByType(config.getType());
                HadoopFileSystemOperations.addClasspath(yarnConfiguration, flow,
                        versionManager.getCurrentVersionInStack(stackName));
                return flow;
            }
        }
        return null;
    }

}
