package com.latticeengines.leadprioritization.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component
public class PlayLaunchProcessor {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchProcessor.class);

    @Autowired
    private AccountFetcher accountFetcher;

    @Autowired
    private ContactFetcher contactFetcher;

    @Autowired
    private RecommendationCreator recommendationCreator;

    @Autowired
    private FrontEndQueryCreator frontEndQueryCreator;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private long segmentAccountsCount = 0;

    private long processedSegmentAccountsCount = 0;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public void executeLaunchActivity(Tenant tenant, PlayLaunchInitStepConfiguration config)
            throws JsonProcessingException {
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        PlayLaunch playLaunch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
        Play play = internalResourceRestApiProxy.findPlayByName(customerSpace, playName);
        playLaunch.setPlay(play);
        long launchTimestampMillis = playLaunch.getCreated().getTime();

        RatingEngine ratingEngine = play.getRatingEngine();
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Rating Engine for play %s cannot be null", play.getName()));
        }
        MetadataSegment segment = ratingEngine.getSegment();
        if (segment == null) {
            throw new NullPointerException(
                    String.format("Segment for Rating Engine %s cannot be null", ratingEngine.getId()));
        }
        String segmentName = segment.getName();
        log.info(String.format("Processing segment: %s", segmentName));
        FrontEndQuery accountFrontEndQuery = new FrontEndQuery();
        FrontEndQuery contactFrontEndQuery = new FrontEndQuery();
        List<Object> modifiableAccountIdCollectionForContacts = new ArrayList<>();

        String modelId = frontEndQueryCreator.prepareFrontEndQueries(customerSpace, play, segmentName,
                accountFrontEndQuery, contactFrontEndQuery, modifiableAccountIdCollectionForContacts);

        segmentAccountsCount = accountFetcher.getCount(config, accountFrontEndQuery);
        log.info(String.format("Total records in segment: %d", segmentAccountsCount));

        long suppressedAccounts = 0;
        if (accountFrontEndQuery.restrictNotNullSalesforceId()) {
            accountFrontEndQuery.setRestrictNotNullSalesforceId(false);
            suppressedAccounts = accountFetcher.getCount(config, accountFrontEndQuery) - segmentAccountsCount;
            accountFrontEndQuery.setRestrictNotNullSalesforceId(true);
        }
        playLaunch.setAccountsSuppressed(suppressedAccounts);

        if (segmentAccountsCount > 0) {
            AtomicLong accountLaunched = new AtomicLong();
            AtomicLong contactLaunched = new AtomicLong();
            AtomicLong accountErrored = new AtomicLong();
            AtomicLong accountSuppressed = new AtomicLong(suppressedAccounts);

            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);

            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            for (int pageNo = 0; pageNo < pages; pageNo++) {
                log.info(String.format("Loop #%d", pageNo));

                DataPage accountsPage = //
                        accountFetcher.fetch(//
                                config, accountFrontEndQuery, segmentAccountsCount, //
                                processedSegmentAccountsCount);

                processAccountsPage(tenant, playLaunch, config, contactFrontEndQuery, //
                        modifiableAccountIdCollectionForContacts, modelId, //
                        accountLaunched, contactLaunched, accountErrored, accountSuppressed, accountsPage,
                        launchTimestampMillis);

                updateLaunchProgress(tenant, playLaunch, accountLaunched, contactLaunched, accountErrored);
            }
        }
    }

    private void updateLaunchProgress(Tenant tenant, PlayLaunch playLaunch, AtomicLong accountLaunched,
            AtomicLong contactLaunched, AtomicLong accountErrored) {
        playLaunch.setLaunchCompletionPercent(100 * processedSegmentAccountsCount / segmentAccountsCount);
        playLaunch.setAccountsLaunched(accountLaunched.get());
        playLaunch.setContactsLaunched(contactLaunched.get());
        playLaunch.setAccountsErrored(accountErrored.get());
        updateLaunchProgress(tenant, playLaunch);
        log.info("launch progress: " + playLaunch.getLaunchCompletionPercent() + "% completed");
    }

    private void processAccountsPage(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            FrontEndQuery contactFrontEndQuery, List<Object> modifiableAccountIdCollectionForContacts, String modelId,
            AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, DataPage accountsPage, long launchTimestampMillis) {
        List<Map<String, Object>> accountList = accountsPage.getData();

        if (CollectionUtils.isNotEmpty(accountList)) {

            List<Object> accountIds = getAccountsIds(accountList);

            modifiableAccountIdCollectionForContacts.clear();
            modifiableAccountIdCollectionForContacts.addAll(accountIds);
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList = new HashMap<>();

            contactFetcher.fetch(config, contactFrontEndQuery, mapForAccountAndContactList);

            recommendationCreator.generateRecommendations(tenant, playLaunch, config, //
                    modelId, accountLaunched, contactLaunched, accountErrored, //
                    accountSuppressed, accountList, mapForAccountAndContactList, launchTimestampMillis);
        }
        processedSegmentAccountsCount += accountList.size();
    }

    private void updateLaunchProgress(Tenant tenant, PlayLaunch playLaunch) {
        try {
            internalResourceRestApiProxy.updatePlayLaunchProgress(tenant.getId(), //
                    playLaunch.getPlay().getName(), playLaunch.getLaunchId(), playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsLaunched(), playLaunch.getContactsLaunched(), playLaunch.getAccountsErrored(),
                    playLaunch.getAccountsSuppressed());
        } catch (Exception e) {
            log.error("Unable to update launch progress.", e);
        }
    }

    private List<Object> getAccountsIds(List<Map<String, Object>> accountList) {
        List<Object> accountIds = //
                accountList//
                        .stream().parallel() //
                        .map( //
                                account -> account.get(InterfaceName.AccountId.name()) //
                        ) //
                        .collect(Collectors.toList());

        log.info(String.format("Extracting contacts for accountIds: %s",
                Arrays.deepToString(accountIds.toArray(new Object[accountIds.size()]))));
        return accountIds;
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    void setAccountFetcher(AccountFetcher accountFetcher) {
        this.accountFetcher = accountFetcher;
    }

    @VisibleForTesting
    void setContactFetcher(ContactFetcher contactFetcher) {
        this.contactFetcher = contactFetcher;
    }

    @VisibleForTesting
    void setRecommendationCreator(RecommendationCreator recommendationCreator) {
        this.recommendationCreator = recommendationCreator;
    }

    @VisibleForTesting
    void setFrontEndQueryCreator(FrontEndQueryCreator frontEndQueryCreator) {
        this.frontEndQueryCreator = frontEndQueryCreator;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }
}
