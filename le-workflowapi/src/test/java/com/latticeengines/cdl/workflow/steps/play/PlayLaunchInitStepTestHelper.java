package com.latticeengines.cdl.workflow.steps.play;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.yarn.exposed.service.JobService;

public class PlayLaunchInitStepTestHelper {

    private RecommendationCreator recommendationCreator;
    private PlayLaunchProcessor playLaunchProcessor;
    private AccountFetcher accountFetcher;
    private ContactFetcher contactFetcher;
    private FrontEndQueryCreator frontEndQueryCreator;

    public PlayLaunchInitStepTestHelper(InternalResourceRestApiProxy internalResourceRestApiProxy,
            EntityProxy entityProxy, RecommendationService recommendationService, long pageSize,
            MetadataProxy metadataProxy, SqoopProxy sqoopProxy, JobService jobService, String dataDbDriver,
            String dataDbUrl, String dataDbUser, String dataDbPassword, String dataDbDialect, String dataDbType,
            Configuration yarnConfiguration) {
        frontEndQueryCreator = new FrontEndQueryCreator();
        frontEndQueryCreator.initLookupFieldsConfiguration();

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
        playLaunchProcessor.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
        playLaunchProcessor.setMetadataProxy(metadataProxy);
        playLaunchProcessor.setSqoopProxy(sqoopProxy);
        playLaunchProcessor.setJobService(jobService);
        playLaunchProcessor.setDataDbDriver(dataDbDriver);
        playLaunchProcessor.setDataDbUrl(dataDbUrl);
        playLaunchProcessor.setDataDbUser(dataDbUser);
        playLaunchProcessor.setDataDbPassword(dataDbPassword);
        playLaunchProcessor.setDataDbDialect(dataDbDialect);
        playLaunchProcessor.setDataDbType(dataDbType);
        playLaunchProcessor.setYarnConfiguration(yarnConfiguration);
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
