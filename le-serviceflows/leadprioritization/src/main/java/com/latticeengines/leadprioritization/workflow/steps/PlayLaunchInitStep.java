package com.latticeengines.leadprioritization.workflow.steps;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private RecommendationService recommendationService;

    @Override
    public void execute() {
        Tenant tenant = null;
        PlayLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        try {
            log.info("Inside PlayLaunchInitStep execute()");
            tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info("For tenant: " + customerSpace.toString());
            log.info("For playId: " + playName);
            log.info("For playLaunchId: " + playLaunchId);

            PlayLaunch playLauch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);

            executeLaunchActivity(tenant, playLauch, config);

            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
        } catch (Exception ex) {
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
        }

    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLauch, PlayLaunchInitStepConfiguration config) {
        // add processing logic

        // DUMMY LOGIC TO TEST INTEGRATION WITH recommendationService

        for (int i = 0; i < 3; i++) {
            Recommendation recommendation = createDummyRecommendation(tenant, playLauch, config);
            recommendationService.create(recommendation);
        }
    }

    private Recommendation createDummyRecommendation(Tenant tenant, PlayLaunch playLauch,
            PlayLaunchInitStepConfiguration config) {
        Random rand = new Random();
        String ACCOUNT_ID = "acc__" + System.currentTimeMillis() + rand.nextInt(50000);

        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLauch.getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);
        recommendation.setLaunchDate(playLauch.getCreatedTimestamp());
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(Math.min(0.5D, 1 / rand.nextDouble()));
        recommendation.setSynchronizationDestination("SFDC");
        recommendation.setPriorityDisplayName("A");

        return recommendation;
    }
}
