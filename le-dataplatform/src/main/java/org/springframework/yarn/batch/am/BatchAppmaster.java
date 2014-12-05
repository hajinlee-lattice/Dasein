package org.springframework.yarn.batch.am;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.RackResolver;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.batch.event.JobExecutionEvent;
import org.springframework.yarn.batch.partition.AbstractPartitionHandler;
import org.springframework.yarn.batch.repository.BatchAppmasterService;
import org.springframework.yarn.batch.repository.JobRepositoryRemoteServiceInterceptor;
import org.springframework.yarn.batch.repository.JobRepositoryRpcFactory;
import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusReq;
import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusRes;
import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.event.AbstractYarnEvent;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.runtime.metric.LedpMetricsMgr;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class BatchAppmaster extends AbstractBatchAppmaster implements YarnAppmaster {

    private static final Log log = LogFactory.getLog(BatchAppmaster.class);

    @Autowired(required = false)
    private final Collection<PartitionHandler> partitionHandlers = Collections.emptySet();

    @Autowired
    private Configuration yarnConfiguration;

    private List<JobExecution> jobExecutions = new ArrayList<JobExecution>();

    private LedpMetricsMgr ledpMetricsMgr = null;
    
    private ProgressMonitor monitor;

    private String priority;

    private String customer;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Autowired
    protected YarnService yarnService;

    @Override
    protected void onInit() throws Exception {
        super.onInit();
        if (getLauncher() instanceof AbstractLauncher) {
            ((AbstractLauncher) getLauncher()).addInterceptor(this);
        }
        RackResolver.init(getConfiguration());
        monitor = new ProgressMonitor(super.getAllocator());
    }

    @Override
    public void setParameters(Properties parameters) {
        if (parameters == null) {
            return;
        }
        for (Map.Entry<Object, Object> parameter : parameters.entrySet()) {
            log.info("Key = " + parameter.getKey().toString() + " Value = " + parameter.getValue().toString());
        }
        super.setParameters(parameters);

        priority = parameters.getProperty(ContainerProperty.PRIORITY.name());
        if (priority == null) {
            throw new LedpException(LedpCode.LEDP_12000);
        }
        customer = parameters.getProperty(AppMasterProperty.CUSTOMER.name());
        if (customer == null) {
            throw new LedpException(LedpCode.LEDP_12007);
        }
        try {
            setRuntimeConfig(getParameters());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void submitApplication() {
        log.info("Submitting application.");
        registerAppmaster();
        start();
        ApplicationAttemptId appAttemptId = getApplicationAttemptId();
        if (getAllocator() instanceof AbstractAllocator) {
            log.info("Setting application attempt id " + appAttemptId);
            ((AbstractAllocator) getAllocator()).setApplicationAttemptId(appAttemptId);
        }
        final String appId = appAttemptId.getApplicationId().toString();
        ledpMetricsMgr = LedpMetricsMgr.getInstance(appAttemptId.toString());
        ledpMetricsMgr.setPriority(priority);
        ledpMetricsMgr.setCustomer(customer);
        final long appStartTime = System.currentTimeMillis();
        ledpMetricsMgr.setAppStartTime(appStartTime);

        log.info("Application submitted with app id = " + appId);
        new Thread(new Runnable() {

            @Override
            public void run() {
                AppInfo appInfo = yarnService.getApplication(appId);

                String queue = appInfo.getQueue();
                if (queue == null) {
                    throw new LedpException(LedpCode.LEDP_12006);
                }
                ledpMetricsMgr.setQueue(queue);
                ledpMetricsMgr.start();

                long appSubmissionTime = appInfo.getStartTime();
                log.info("App start latency = " + (appStartTime - appSubmissionTime));
                ledpMetricsMgr.setAppSubmissionTime(appSubmissionTime);
            }
        }).start();

        for (PartitionHandler handler : partitionHandlers) {
            if (handler instanceof AbstractPartitionHandler) {
                ((AbstractPartitionHandler) handler).setBatchAppmaster(this);
            }
        }

        try {
            getYarnJobLauncher().run(getParameters());
        } catch (JobExecutionException e) {
            log.error("Error in jobLauncherHelper.", e);
            setFinalApplicationStatus(FinalApplicationStatus.FAILED);
        }
        for (JobExecution jobExecution : jobExecutions) {
            if (jobExecution.getStatus().equals(BatchStatus.FAILED)) {
                setFinalApplicationStatus(FinalApplicationStatus.FAILED);
                break;
            }
        }
        ledpMetricsMgr.publishMetricsNow();
        notifyCompleted();
    }

    @Override
    public void onApplicationEvent(AbstractYarnEvent event) {
        super.onApplicationEvent(event);
        if (event instanceof JobExecutionEvent) {
            jobExecutions.add(((JobExecutionEvent) event).getJobExecution());
        }
    }

    @Override
    protected void doStart() {
        super.doStart();

        AppmasterService service = getAppmasterService();
        if (log.isDebugEnabled() && service != null) {
            log.debug("Appmaster service " + service + " started");
        }

        if (service instanceof BatchAppmasterService) {
            ((BatchAppmasterService) service).addInterceptor(new JobRepositoryRemoteServiceInterceptor() {

                @Override
                public BaseObject preRequest(BaseObject baseObject) {
                    if (baseObject.getType().equals("PartitionedStepExecutionStatusReq")) {
                        StepExecutionType stepExecutionType = ((PartitionedStepExecutionStatusReq) baseObject).stepExecution;
                        StepExecution convertStepExecution = JobRepositoryRpcFactory
                                .convertStepExecutionType(stepExecutionType);
                        getStepExecutions().add(convertStepExecution);
                        return null;
                    } else {
                        return baseObject;
                    }
                }

                @Override
                public BaseResponseObject postRequest(BaseResponseObject baseResponseObject) {
                    return baseResponseObject;
                }

                @Override
                public BaseResponseObject handleRequest(BaseObject baseObject) {
                    return new PartitionedStepExecutionStatusRes();
                }
            });
        }

        if (service != null && service.hasPort()) {
            for (int i = 0; i < 10; i++) {
                if (service.getPort() == -1) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                } else {
                    break;
                }
            }
        }

        if (getAppmasterService() instanceof SmartLifecycle) {
            ((SmartLifecycle) getAppmasterService()).start();
        }
        monitor.start();
    }

    private void setRuntimeConfig(Properties parameters) throws IOException {
        // Sets runtime host and port needed by python
        File progressConfig = new File(System.getProperty("user.dir") + "/" + runtimeConfig);
        log.info(progressConfig.getAbsolutePath());
        List<String> lines = new ArrayList<>();
        lines.add("host=" + monitor.getHost());
        lines.add("port=" + Integer.toString(monitor.getPort()));
        FileUtils.writeLines(progressConfig, lines);
        log.info("Writing runtime host: " + monitor.getHost() + " port: " + monitor.getPort());
    }

    @Override
    public void doStop() {
        super.doStop();
        cleanupJobDir();
        monitor.stop();
        
        if (ledpMetricsMgr != null) {
            ledpMetricsMgr.setAppEndTime(System.currentTimeMillis());
            ledpMetricsMgr.publishMetricsNow();
        }
    }

    private void cleanupJobDir() {
        String dir = hdfsJobBaseDir + "/" + getParameters().getProperty(ContainerProperty.JOBDIR.name());
        try {
            HdfsUtils.rmdir(yarnConfiguration, dir);
        } catch (Exception e) {
            log.warn("Could not delete job dir " + dir + ".", e);
        }

    }
}
