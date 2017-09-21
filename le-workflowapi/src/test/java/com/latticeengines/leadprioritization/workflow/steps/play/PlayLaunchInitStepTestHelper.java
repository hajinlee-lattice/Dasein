package com.latticeengines.leadprioritization.workflow.steps.play;

import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class PlayLaunchInitStepTestHelper {

    private RecommendationCreator recommendationCreator;
    private PlayLaunchProcessor playLaunchProcessor;
    private AccountFetcher accountFetcher;
    private ContactFetcher contactFetcher;
    private FrontEndQueryCreator frontEndQueryCreator;

    public PlayLaunchInitStepTestHelper(InternalResourceRestApiProxy internalResourceRestApiProxy,
            EntityProxy entityProxy, RecommendationService recommendationService, DanteLeadProxy danteLeadProxy,
            long pageSize) {
        frontEndQueryCreator = new FrontEndQueryCreator();
        frontEndQueryCreator.initLookupFieldsConfiguration();

        recommendationCreator = new RecommendationCreator();
        recommendationCreator.setRecommendationService(recommendationService);
        recommendationCreator.setDanteLeadProxy(danteLeadProxy);

        accountFetcher = new AccountFetcher();
        accountFetcher.setPageSize(pageSize);
        accountFetcher.setEntityProxy(entityProxy);

        contactFetcher = new ContactFetcher();
        contactFetcher.setPageSize(pageSize);
        contactFetcher.setEntityProxy(entityProxy);

        playLaunchProcessor = new PlayLaunchProcessor();
        playLaunchProcessor.setPageSize(pageSize);
        playLaunchProcessor.setAccountFetcher(accountFetcher);
        playLaunchProcessor.setContactFetcher(contactFetcher);
        playLaunchProcessor.setFrontEndQueryCreator(frontEndQueryCreator);
        playLaunchProcessor.setRecommendationCreator(recommendationCreator);
        playLaunchProcessor.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
    }

    public RecommendationCreator getRecommendationCreator() {
        return recommendationCreator;
    }

    public PlayLaunchProcessor getPlayLaunchProcessor() {
        return playLaunchProcessor;
    }

    public AccountFetcher getAccountFetcher() {
        return accountFetcher;
    }

    public ContactFetcher getContactFetcher() {
        return contactFetcher;
    }

    public FrontEndQueryCreator getFrontEndQueryCreator() {
        return frontEndQueryCreator;
    }

}
