package com.latticeengines.propdata.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("parallelExecution")
@Scope("prototype")
public class ParallelExecution extends BaseWorkflowStep<ParallelExecutionConfiguration> {

    private static Log log = LogFactory.getLog(ParallelExecution.class);

    @Autowired
    private InternalProxy internalProxy;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        log.info("Inside ParallelExecution execute()");
        List<PropDataJobConfiguration> jobConfigurations = (List<PropDataJobConfiguration>) executionContext
                .get(BulkMatchContextKey.YARN_JOB_CONFIGS);

        List<ApplicationId> applicationIds = new ArrayList<>();
        for (PropDataJobConfiguration jobConfiguration : jobConfigurations) {
            ApplicationId appId = ConverterUtils
                    .toApplicationId(internalProxy.submitYarnJob(jobConfiguration).getApplicationIds().get(0));
            log.info("Submit a match block to application id " + appId);
            applicationIds.add(appId);
        }

        LogManager.getLogger(TimelineClientImpl.class).setLevel(Level.WARN);
        LogManager.getLogger(RMProxy.class).setLevel(Level.WARN);
        LogManager.getLogger(AHSProxy.class).setLevel(Level.WARN);

        for (ApplicationId appId: applicationIds) {
            YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 3600 * 24);
        }

        LogManager.getLogger(TimelineClientImpl.class).setLevel(Level.INFO);
        LogManager.getLogger(RMProxy.class).setLevel(Level.INFO);
        LogManager.getLogger(AHSProxy.class).setLevel(Level.INFO);
    }

}
