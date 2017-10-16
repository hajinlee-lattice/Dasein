package com.latticeengines.leadprioritization.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.play.PlayLaunchContext.Counter;
import com.latticeengines.leadprioritization.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
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

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public void executeLaunchActivity(Tenant tenant, PlayLaunchInitStepConfiguration config)
            throws JsonProcessingException {
        // initialize play launch context
        PlayLaunchContext playLaunchContext = initPlayLaunchContext(tenant, config);

        // prepare account and contact front end queries
        frontEndQueryCreator.prepareFrontEndQueries(playLaunchContext);

        long segmentAccountsCount = accountFetcher.getCount(playLaunchContext);
        log.info(String.format("Total records in segment: %d", segmentAccountsCount));
        playLaunchContext.getPlayLaunch().setAccountsSelected(segmentAccountsCount);

        // do initial handling of SFDC id based suppression
        handleSFDCIdBasedSuppression(playLaunchContext, segmentAccountsCount);

        if (segmentAccountsCount > 0) {
            // process accounts that exists in segment
            long processedSegmentAccountsCount = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);
            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            // loop over to required number of pages
            for (int pageNo = 0; pageNo < pages; pageNo++) {
                // fetch and process a single page
                processedSegmentAccountsCount = fetchAndProcessPage(playLaunchContext, segmentAccountsCount,
                        processedSegmentAccountsCount, pageNo);
            }
        }

        if (playLaunchContext.getPlayLaunch().getAccountsErrored() != null
                && playLaunchContext.getPlayLaunch().getAccountsErrored() > 0) {
            throw new RuntimeException(String.format("Encountered %d errors while processing accounts for play launch",
                    playLaunchContext.getPlayLaunch().getAccountsErrored()));
        }
    }

    private void handleSFDCIdBasedSuppression(PlayLaunchContext playLaunchContext, long segmentAccountsCount) {
        long suppressedAccounts = 0;
        playLaunchContext.getPlayLaunch().setAccountsSuppressed(suppressedAccounts);

        FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
        if (accountFrontEndQuery.restrictNotNullSalesforceId()) {
            accountFrontEndQuery.setRestrictNotNullSalesforceId(false);
            suppressedAccounts = accountFetcher.getCount(playLaunchContext) - segmentAccountsCount;
            playLaunchContext.getPlayLaunch().setAccountsSuppressed(suppressedAccounts);
            accountFrontEndQuery.setRestrictNotNullSalesforceId(true);
        }
        playLaunchContext.getCounter().getAccountSuppressed().set(suppressedAccounts);
    }

    private long fetchAndProcessPage(PlayLaunchContext playLaunchContext, long segmentAccountsCount,
            long processedSegmentAccountsCount, int pageNo) {
        log.info(String.format("Loop #%d", pageNo));

        // fetch accounts in current page
        DataPage accountsPage = //
                accountFetcher.fetch(//
                        playLaunchContext, segmentAccountsCount, processedSegmentAccountsCount);

        // process accounts in current page
        processedSegmentAccountsCount += processAccountsPage(playLaunchContext, accountsPage);

        // update launch progress
        updateLaunchProgress(playLaunchContext, processedSegmentAccountsCount, segmentAccountsCount);
        return processedSegmentAccountsCount;
    }

    private PlayLaunchContext initPlayLaunchContext(Tenant tenant, PlayLaunchInitStepConfiguration config) {
        PlayLaunchContextBuilder playLaunchContextBuilder = new PlayLaunchContextBuilder();

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

        String modelId = null;
        for (RatingModel model : ratingEngine.getRatingModels()) {
            modelId = model.getId();
            break;
        }

        String segmentName = segment.getName();
        log.info(String.format("Processing segment: %s", segmentName));

        playLaunchContextBuilder //
                .customerSpace(customerSpace) //
                .tenant(tenant) //
                .launchTimestampMillis(launchTimestampMillis) //
                .play(play) //
                .playLaunch(playLaunch) //
                .playLaunchId(playLaunchId) //
                .playName(playName) //
                .ratingEngine(ratingEngine) //
                .segment(segment) //
                .segmentName(segmentName) //
                .accountFrontEndQuery(new FrontEndQuery()) //
                .contactFrontEndQuery(new FrontEndQuery()) //
                .modelId(modelId) //
                .modifiableAccountIdCollectionForContacts(new ArrayList<>()) //
                .counter(new Counter());

        return playLaunchContextBuilder.build();
    }

    private void updateLaunchProgress(PlayLaunchContext playLaunchContext, long processedSegmentAccountsCount,
            long segmentAccountsCount) {
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();

        playLaunch.setLaunchCompletionPercent(100 * processedSegmentAccountsCount / segmentAccountsCount);
        playLaunch.setAccountsLaunched(playLaunchContext.getCounter().getAccountLaunched().get());
        playLaunch.setContactsLaunched(playLaunchContext.getCounter().getContactLaunched().get());
        playLaunch.setAccountsErrored(playLaunchContext.getCounter().getAccountErrored().get());

        updateLaunchProgress(playLaunchContext);
        log.info("launch progress: " + playLaunch.getLaunchCompletionPercent() + "% completed");
    }

    private long processAccountsPage(PlayLaunchContext playLaunchContext, DataPage accountsPage) {
        List<Object> modifiableAccountIdCollectionForContacts = playLaunchContext
                .getModifiableAccountIdCollectionForContacts();

        List<Map<String, Object>> accountList = accountsPage.getData();

        if (CollectionUtils.isNotEmpty(accountList)) {

            List<Object> accountIds = getAccountsIds(accountList);

            // make sure to clear list of account Ids in contact query and then
            // insert list of accounts ids from current account page
            modifiableAccountIdCollectionForContacts.clear();
            modifiableAccountIdCollectionForContacts.addAll(accountIds);

            // fetch corresponding contacts and prepare map of accountIs vs list
            // of contacts
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList = //
                    contactFetcher.fetch(playLaunchContext);

            // generate recommendations using list of accounts in page and
            // corresponding account/contacts map
            recommendationCreator.generateRecommendations(playLaunchContext, //
                    accountList, mapForAccountAndContactList);
        }
        return accountList.size();
    }

    private void updateLaunchProgress(PlayLaunchContext playLaunchContext) {
        try {
            PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();

            internalResourceRestApiProxy.updatePlayLaunchProgress(playLaunchContext.getCustomerSpace(), //
                    playLaunch.getPlay().getName(), playLaunch.getLaunchId(), playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsSelected(), playLaunch.getAccountsLaunched(),
                    playLaunch.getContactsLaunched(), playLaunch.getAccountsErrored(),
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
