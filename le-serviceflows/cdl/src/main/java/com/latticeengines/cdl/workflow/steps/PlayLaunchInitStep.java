package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchProcessor;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("playLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private PlayLaunchProcessor playLaunchProcessor;

    @Autowired
    private PlayProxy playProxy;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    @Override
    public void execute() {
        PlayLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        try {
            log.info("Inside PlayLaunchInitStep execute()");
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info(String.format("For tenant: %s", customerSpace.toString()));
            log.info(String.format("For playId: %s", playName));
            log.info(String.format("For playLaunchId: %s", playLaunchId));

            String recAvroHdfsFilePath = playLaunchProcessor.launchPlay(tenant, config);
            putStringValueInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH,
                    recAvroHdfsFilePath);

            successUpdates(customerSpace, playName, playLaunchId);
        } catch (Exception ex) {
            failureUpdates(customerSpace, playName, playLaunchId, ex);
            throw new LedpException(LedpCode.LEDP_18157, ex);
        }
    }

    private void failureUpdates(CustomerSpace customerSpace, String playName, String playLaunchId, Exception ex) {
        log.error(ex.getMessage(), ex);
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Failed);
    }

    private void successUpdates(CustomerSpace customerSpace, String playName, String playLaunchId) {
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Launched);
        playProxy.publishTalkingPoints(customerSpace.toString(), playName);
    }

    @VisibleForTesting
    void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }

    @VisibleForTesting
    void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

    @VisibleForTesting
    void setPlayLaunchProcessor(PlayLaunchProcessor playLaunchProcessor) {
        this.playLaunchProcessor = playLaunchProcessor;
    }

}
