package com.latticeengines.leadprioritization.workflow.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class PlayLaunchInitStepUnitTestNG {

    private PlayLaunchInitStep playLaunchInitStep;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    @Mock
    EntityProxy entityProxy;

    @Mock
    DataCollectionProxy dataCollectionProxy;

    @Mock
    InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Mock
    RecommendationService recommendationService;

    @Mock
    TalkingPointProxy talkingPointProxy;

    @Mock
    TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "unit")
    public void setup() {
        String randId = UUID.randomUUID().toString();
        String tenantIdentifier = randId + "." + randId + ".Production";
        String playId = "play__" + randId;
        String playLaunchId = "launch__" + randId;
        long pageSize = 2l;

        MockitoAnnotations.initMocks(this);

        mockAccountProxy(pageSize);

        mockInternalResource(playId, playLaunchId);

        mockTenantMgr(new Tenant(tenantIdentifier));

        mockRecommendationService();

        mockTalkingPointProxy();

        playLaunchInitStep = new PlayLaunchInitStep();

        playLaunchInitStep.setEntityProxy(entityProxy);
        playLaunchInitStep.setDataCollectionProxy(dataCollectionProxy);
        playLaunchInitStep.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
        playLaunchInitStep.setPageSize(pageSize);
        playLaunchInitStep.setRecommendationService(recommendationService);
        playLaunchInitStep.setTalkingPointProxy(talkingPointProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);

        playLaunchInitStep.setConfiguration(createConf(CustomerSpace.parse(tenantIdentifier), playId, playLaunchId));
    }

    @Test(groups = "unit")
    public void testExecute() {
        playLaunchInitStep.execute();
    }

    private PlayLaunchInitStepConfiguration createConf(CustomerSpace customerSpace, String playName,
            String playLaunchId) {
        PlayLaunchInitStepConfiguration config = new PlayLaunchInitStepConfiguration();
        config.setCustomerSpace(customerSpace);
        config.setPlayLaunchId(playLaunchId);
        config.setPlayName(playName);
        return config;
    }

    private void mockTalkingPointProxy() {
        doNothing() //
                .when(talkingPointProxy) //
                .publish(anyString(), anyString());
    }

    private void mockRecommendationService() {
        doNothing() //
                .when(recommendationService) //
                .create(any(Recommendation.class));
    }

    private void mockTenantMgr(Tenant tenant) {
        when(tenantEntityMgr.findByTenantId( //
                anyString())) //
                        .thenReturn(tenant);
    }

    private void mockInternalResource(String playId, String playLaunchId) {

        when(internalResourceRestApiProxy.findPlayByName( //
                any(CustomerSpace.class), //
                anyString())) //
                        .thenReturn(createPlay(playId));

        when(internalResourceRestApiProxy.getPlayLaunch( //
                any(CustomerSpace.class), //
                anyString(), //
                anyString())) //
                        .thenReturn(createPlayLaunch(playId, playLaunchId));

        when(internalResourceRestApiProxy.getSegmentRestrictionQuery( //
                any(CustomerSpace.class), //
                anyString())) //
                        .thenReturn(createSegmentRestrictionQuery());

        doNothing() //
                .when(internalResourceRestApiProxy) //
                .updatePlayLaunch( //
                        any(CustomerSpace.class), //
                        anyString(), //
                        anyString(), //
                        any(LaunchState.class));
    }

    private void mockAccountProxy(long pageSize) {
        when(entityProxy.getCount( //
                anyString(), //
                any(FrontEndQuery.class))) //
                        .thenReturn(2l);

        when(entityProxy.getData( //
                anyString(), //
                any(FrontEndQuery.class))) //
                        .thenReturn(generateDataPage(pageSize));
    }

    private Restriction createSegmentRestrictionQuery() {
        return null;// new RestrictionBuilder().build();
    }

    private Play createPlay(String playId) {
        Play play = new Play();
        play.setName(playId);
        return play;
    }

    private PlayLaunch createPlayLaunch(String playId, String playLaunchId) {
        PlayLaunch launch = new PlayLaunch();
        launch.setPlay(createPlay(playId));
        launch.setLaunchId(playLaunchId);
        return launch;
    }

    private DataPage generateDataPage(long pageSize) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (long i = 0; i < pageSize; i++) {
            Map<String, Object> data = new HashMap<>();
            dataList.add(data);
        }
        DataPage dataPage = new DataPage(dataList);
        return dataPage;
    }

}
