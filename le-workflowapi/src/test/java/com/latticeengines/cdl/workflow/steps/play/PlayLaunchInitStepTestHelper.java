package com.latticeengines.cdl.workflow.steps.play;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.yarn.exposed.service.JobService;

public class PlayLaunchInitStepTestHelper {

    private RecommendationCreator recommendationCreator;
    private PlayLaunchProcessor playLaunchProcessor;
    private AccountFetcher accountFetcher;
    private ContactFetcher contactFetcher;
    private FrontEndQueryCreator frontEndQueryCreator;

    public PlayLaunchInitStepTestHelper(PlayProxy playProxy, LookupIdMappingProxy lookupIdMappingProxy,
            EntityProxy entityProxy, RecommendationService recommendationService, long pageSize,
            MetadataProxy metadataProxy, SqoopProxy sqoopProxy, RatingEngineProxy ratingEngineProxy,
            JobService jobService, DataCollectionProxy dataCollectionProxy, String dataDbDriver, String dataDbUrl,
            String dataDbUser, String dataDbPassword, String dataDbDialect, String dataDbType,
            Configuration yarnConfiguration, BatonService batonService) {
        frontEndQueryCreator = new FrontEndQueryCreator();
        frontEndQueryCreator.initLookupFieldsConfiguration();
        frontEndQueryCreator.setBatonService(batonService);

        recommendationCreator = new RecommendationCreator();

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
        playLaunchProcessor.setLookupIdMappingProxy(lookupIdMappingProxy);
        playLaunchProcessor.setPlayProxy(playProxy);
        playLaunchProcessor.setMetadataProxy(metadataProxy);
        playLaunchProcessor.setSqoopProxy(sqoopProxy);
        playLaunchProcessor.setRatingEngineProxy(ratingEngineProxy);
        playLaunchProcessor.setJobService(jobService);
        playLaunchProcessor.setDataCollectionProxy(dataCollectionProxy);
        playLaunchProcessor.setDataDbDriver(dataDbDriver);
        playLaunchProcessor.setDataDbUrl(dataDbUrl);
        playLaunchProcessor.setDataDbUser(dataDbUser);
        playLaunchProcessor.setDataDbPassword(dataDbPassword);
        playLaunchProcessor.setDataDbDialect(dataDbDialect);
        playLaunchProcessor.setDataDbType(dataDbType);
        playLaunchProcessor.setYarnConfiguration(yarnConfiguration);
        playLaunchProcessor.setBatonService(batonService);
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
