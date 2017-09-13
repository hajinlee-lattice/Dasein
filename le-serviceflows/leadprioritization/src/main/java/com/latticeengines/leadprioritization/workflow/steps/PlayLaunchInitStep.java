package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private RecommendationService recommendationService;

    @Autowired
    private EntityProxy entityProxy;

    @Autowired
    private DanteLeadProxy danteLeadProxy;

    private long launchTimestampMillis;

    private long segmentAccountsCount = 0;

    private long processedSegmentAccountsCount = 0;

    private Map<BusinessEntity, List<String>> accountLookupFields;

    private Map<BusinessEntity, List<String>> contactLookupFields;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        initLookupFieldsConfiguration();

    }

    @Override
    public void execute() {
        Tenant tenant;
        PlayLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        launchTimestampMillis = System.currentTimeMillis();

        try {
            log.info("Inside PlayLaunchInitStep execute()");
            tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info(String.format("For tenant: %s", customerSpace.toString()));
            log.info(String.format("For playId: %s", playName));
            log.info(String.format("For playLaunchId: %s", playLaunchId));

            PlayLaunch playLaunch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
            Play play = internalResourceRestApiProxy.findPlayByName(customerSpace, playName);
            playLaunch.setPlay(play);

            RatingEngine ratingEngine = play.getRatingEngine();
            if (ratingEngine == null) {
                throw new NullPointerException(
                        String.format("Rating Engine for play %s cannot be null", play.getName()));
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

            String modelId = prepareFrontEndQueries(customerSpace, play, segmentName, accountFrontEndQuery,
                    contactFrontEndQuery, modifiableAccountIdCollectionForContacts);

            executeLaunchActivity(tenant, playLaunch, config, accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts, modelId);

            successUpdates(customerSpace, playName, playLaunchId);
        } catch (Exception ex) {
            failureUpdates(customerSpace, playName, playLaunchId, ex);
        }
    }

    private void failureUpdates(CustomerSpace customerSpace, String playName, String playLaunchId, Exception ex) {
        log.error(ex.getMessage(), ex);
        internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
    }

    private void successUpdates(CustomerSpace customerSpace, String playName, String playLaunchId) {
        internalResourceRestApiProxy.publishTalkingPoints(customerSpace, playName);
        internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
    }

    private String prepareFrontEndQueries(CustomerSpace customerSpace, Play play, String segmentName,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts) throws JsonProcessingException {
        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, contactFrontEndQuery);

        RatingEngine ratingEngine = play.getRatingEngine();
        String modelId = null;

        if (ratingEngine != null) {
            modelId = prepareQueryUsingRatingsDefn(accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts, ratingEngine);
        } else {
            log.error(String.format("Rating Engine is not set for the play %s", play.getName()));
        }

        if (accountFrontEndQuery.getAccountRestriction() == null) {
            prepareQueryUsingSegmentDefn(customerSpace, segmentName, accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts);
        }

        accountFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
        contactFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
        return modelId;
    }

    private void prepareLookupsForFrontEndQueries(FrontEndQuery accountFrontEndQuery,
            FrontEndQuery contactFrontEndQuery) {
        List<Lookup> accountLookups = new ArrayList<>();
        accountLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, accountLookups, accountLookupFields));

        List<Lookup> contactLookups = new ArrayList<>();
        contactLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, contactLookups, contactLookupFields));

        accountFrontEndQuery.setLookups(accountLookups);
        contactFrontEndQuery.setLookups(contactLookups);
    }

    private void prepareLookups(BusinessEntity businessEntity, List<Lookup> lookups,
            Map<BusinessEntity, List<String>> entityLookupFields) {
        entityLookupFields.get(businessEntity) //
                .stream() //
                .forEach( //
                        field -> lookups.add(new AttributeLookup(businessEntity, field)));
    }

    private void prepareQueryUsingSegmentDefn(CustomerSpace customerSpace, String segmentName,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts) throws JsonProcessingException {
        Restriction segmentRestriction = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                segmentName);

        log.info(String.format("Processing restriction: %s", objectMapper.writeValueAsString(segmentRestriction)));

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(segmentRestriction);
        accountFrontEndQuery.setAccountRestriction(frontEndRestriction);
    }

    private String prepareQueryUsingRatingsDefn(FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts, RatingEngine ratingEngine) {
        List<Lookup> lookups;
        if (ratingEngine.getSegment() != null) {
            FrontEndRestriction accountRestriction = new FrontEndRestriction(
                    ratingEngine.getSegment().getAccountRestriction());
            FrontEndRestriction contactRestriction = new FrontEndRestriction(
                    ratingEngine.getSegment().getContactRestriction());

            accountFrontEndQuery.setAccountRestriction(accountRestriction);
            accountFrontEndQuery.setContactRestriction(contactRestriction);

            Restriction extractedContactRestriction = contactRestriction == null ? new RestrictionBuilder().build()
                    : contactRestriction.getRestriction();
            contactFrontEndQuery.setContactRestriction(
                    prepareContactRestriction(extractedContactRestriction, modifiableAccountIdCollectionForContacts));
        }

        String modelId = null;
        List<RatingModel> ratingModels = new ArrayList<>();
        for (RatingModel model : ratingEngine.getRatingModels()) {
            ratingModels.add(model);
            modelId = model.getId();
            break;
        }
        accountFrontEndQuery.setRatingModels(ratingModels);

        lookups = accountFrontEndQuery.getLookups();
        Lookup lookup = new AttributeLookup(BusinessEntity.Rating, modelId);
        lookups.add(lookup);
        return modelId;
    }

    private FrontEndRestriction prepareContactRestriction(Restriction extractedContactRestriction,
            Collection<Object> modifiableAccountIdCollection) {
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(modifiableAccountIdCollection)
                .build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());

        return frontEndRestriction;
    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts, String modelId) {

        segmentAccountsCount = entityProxy.getCount( //
                config.getCustomerSpace().toString(), //
                accountFrontEndQuery);
        log.info(String.format("Total records in segment: %d", segmentAccountsCount));

        long suppressedAccounts = 0;
        if (accountFrontEndQuery.restrictNotNullSalesforceId()) {
            accountFrontEndQuery.setRestrictNotNullSalesforceId(false);
            suppressedAccounts = entityProxy.getCount(config.getCustomerSpace().toString(), accountFrontEndQuery)
                    - segmentAccountsCount;
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

                DataPage accountsPage = fetchAccountsPage(config, accountFrontEndQuery);

                processAccounts(tenant, playLaunch, config, contactFrontEndQuery, //
                        modifiableAccountIdCollectionForContacts, modelId, //
                        accountLaunched, contactLaunched, accountErrored, accountSuppressed, accountsPage);

                updateLaunchProgress(playLaunch, accountLaunched, contactLaunched, accountErrored);
            }
        }
    }

    private void updateLaunchProgress(PlayLaunch playLaunch, AtomicLong accountLaunched, AtomicLong contactLaunched,
            AtomicLong accountErrored) {
        playLaunch.setLaunchCompletionPercent(100 * processedSegmentAccountsCount / segmentAccountsCount);
        playLaunch.setAccountsLaunched(accountLaunched.get());
        playLaunch.setContactsLaunched(contactLaunched.get());
        playLaunch.setAccountsErrored(accountErrored.get());
        updateLaunchProgress(playLaunch);
        log.info("launch progress: " + playLaunch.getLaunchCompletionPercent() + "% completed");
    }

    private DataPage fetchAccountsPage(PlayLaunchInitStepConfiguration config, FrontEndQuery accountFrontEndQuery) {
        long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));

        accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));

        log.info(String.format("Account query => %s", JsonUtils.serialize(accountFrontEndQuery)));

        DataPage accountPage = entityProxy.getData( //
                config.getCustomerSpace().toString(), //
                accountFrontEndQuery);

        log.info(String.format("Got # %d elements in this loop", accountPage.getData().size()));
        return accountPage;
    }

    private void processAccounts(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            FrontEndQuery contactFrontEndQuery, List<Object> modifiableAccountIdCollectionForContacts, String modelId,
            AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, DataPage accountsPage) {
        List<Map<String, Object>> accountList = accountsPage.getData();

        if (CollectionUtils.isNotEmpty(accountList)) {

            List<Object> accountIds = getAccountsIds(accountList);

            modifiableAccountIdCollectionForContacts.clear();
            modifiableAccountIdCollectionForContacts.addAll(accountIds);
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList = new HashMap<>();

            fetchContacts(config, contactFrontEndQuery, mapForAccountAndContactList);

            generateRecommendations(tenant, playLaunch, config, modelId, //
                    accountLaunched, contactLaunched, accountErrored, accountSuppressed, accountList, //
                    mapForAccountAndContactList);
        }
        processedSegmentAccountsCount += accountList.size();
    }

    private List<Object> getAccountsIds(List<Map<String, Object>> accountList) {
        List<Object> accountIds = //
                parallelStream(accountList) //
                        .map( //
                                account -> account.get(InterfaceName.AccountId.name())) //
                        .collect(Collectors.toList());

        log.info(String.format("Extracting contacts for accountIds: %s",
                Arrays.deepToString(accountIds.toArray(new Object[accountIds.size()]))));
        return accountIds;
    }

    private void fetchContacts(PlayLaunchInitStepConfiguration config, FrontEndQuery contactFrontEndQuery,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList) {
        try {
            contactFrontEndQuery.setPageFilter(null);
            log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));

            Long contactsCount = entityProxy.getCount( //
                    config.getCustomerSpace().toString(), //
                    contactFrontEndQuery);
            int pages = (int) Math.ceil((contactsCount * 1.0D) / pageSize);

            log.info("Number of required loops for fetching contacts: " + pages + ", with pageSize: " + pageSize);
            long processedContactsCount = 0L;

            for (int pageNo = 0; pageNo < pages; pageNo++) {
                processedContactsCount = fetchContactsPage(config, contactFrontEndQuery, mapForAccountAndContactList,
                        contactsCount, processedContactsCount, pageNo);
            }

        } catch (Exception ex) {
            log.error("Ignoring till contact data is available in cdl", ex);
        }
    }

    long fetchContactsPage(PlayLaunchInitStepConfiguration config, FrontEndQuery contactFrontEndQuery,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, Long contactsCount,
            long processedContactsCount, int pageNo) {
        log.info(String.format("Contacts Loop #%d", pageNo));
        long expectedPageSize = Math.min(pageSize, (contactsCount - processedContactsCount));

        contactFrontEndQuery.setPageFilter(new PageFilter(processedContactsCount, expectedPageSize));

        log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));

        DataPage contactPage = entityProxy.getData( //
                config.getCustomerSpace().toString(), //
                contactFrontEndQuery);

        log.info(String.format("Got # %d contact elements in this loop", contactPage.getData().size()));
        processedContactsCount += contactPage.getData().size();

        contactPage //
                .getData().stream() //
                .forEach( //
                        contact -> processContToUpdMapForAccContList(mapForAccountAndContactList, contact));
        return processedContactsCount;
    }

    void processContToUpdMapForAccContList(Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
            Map<String, Object> contact) {
        Object accountIdObj = contact.get(InterfaceName.AccountId.name());

        if (accountIdObj != null) {
            if (!mapForAccountAndContactList.containsKey(accountIdObj)) {
                mapForAccountAndContactList.put(accountIdObj, new ArrayList<>());
            }
            List<Map<String, String>> contacts = mapForAccountAndContactList.get(accountIdObj);
            contacts.add(convertValuesToString(contact));
        }
    }

    private void generateRecommendations(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            String modelId, AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, List<Map<String, Object>> accountList,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList) {
        parallelStream(accountList) //
                .forEach( //
                        account -> {
                            try {
                                processSingleAccount(tenant, playLaunch, config, //
                                        modelId, accountLaunched, contactLaunched, accountErrored, accountSuppressed, //
                                        mapForAccountAndContactList, account);
                            } catch (Throwable th) {
                                log.error(th.getMessage(), th);
                                accountErrored.addAndGet(1);
                                throw th;
                            }
                        });
    }

    private void processSingleAccount(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            String modelId, AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
            Map<String, Object> account) {
        Recommendation recommendation = //
                prepareRecommendation(tenant, playLaunch, config, account, mapForAccountAndContactList, modelId);
        if (playLaunch.getBucketsToLaunch().contains(recommendation.getPriorityID())) {
            recommendationService.create(recommendation);
            danteLeadProxy.create(recommendation, config.getCustomerSpace().toString());
            contactLaunched.addAndGet(
                    recommendation.getExpandedContacts() != null ? recommendation.getExpandedContacts().size() : 0);
            accountLaunched.addAndGet(1);
        } else {
            accountSuppressed.addAndGet(1);
        }
    }

    private Map<String, String> convertValuesToString(Map<String, Object> contact) {
        Map<String, String> contactWithStringValues = new HashMap<>();
        for (String key : contact.keySet()) {
            contactWithStringValues.put(key, contact.get(key) == null ? null : contact.get(key).toString());
        }
        return contactWithStringValues;
    }

    private Stream<Map<String, Object>> parallelStream(List<Map<String, Object>> accountList) {
        return accountList.stream() //
                .parallel();
    }

    private void updateLaunchProgress(PlayLaunch playLaunch) {
        try {
            internalResourceRestApiProxy.updatePlayLaunchProgress(getConfiguration().getCustomerSpace().toString(), //
                    playLaunch.getPlay().getName(), playLaunch.getLaunchId(), playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsLaunched(), playLaunch.getContactsLaunched(), playLaunch.getAccountsErrored(),
                    playLaunch.getAccountsSuppressed());
        } catch (Exception e) {
            log.error("Unable to update launch progress.", e);
        }
    }

    private Recommendation prepareRecommendation(Tenant tenant, PlayLaunch playLaunch,
            PlayLaunchInitStepConfiguration config, Map<String, Object> account,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, String modelId) {
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        Object accountIdObj = checkAndGet(account, InterfaceName.AccountId.name());
        String accountId = accountIdObj == null ? null : accountIdObj.toString();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(accountId);
        recommendation.setLeAccountExternalID(accountId);
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID.name()));
        Double value = 0D;
        recommendation.setMonetaryValue(value);

        // give preference to lattice data cloud field LDC_Name. If not found
        // then try to get company name from customer data itself.
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.LDC_Name.name()));
        if (recommendation.getCompanyName() == null) {
            recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        }

        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(50.0D);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);

        String bucketName = checkAndGet(account, modelId, "A");
        RuleBucketName bucket = null;
        bucket = RuleBucketName.getRuleBucketName(bucketName);

        if (bucket == null) {
            bucket = RuleBucketName.valueOf(bucketName);
        }

        recommendation.setPriorityID(bucket);
        recommendation.setPriorityDisplayName(bucket.getName());

        if (mapForAccountAndContactList.containsKey(accountId)) {
            List<Map<String, String>> contactsForRecommendation = PlaymakerUtils
                    .generateContactForRecommendation(mapForAccountAndContactList.get(accountId));
            recommendation.setExpandedContacts(contactsForRecommendation);
        }

        return recommendation;
    }

    private String checkAndGet(Map<String, Object> account, String columnName) {
        return account.get(columnName) != null ? account.get(columnName).toString() : null;
    }

    private String checkAndGet(Map<String, Object> account, String columnName, String defaultValue) {
        String value = checkAndGet(account, columnName);
        return value == null ? defaultValue : value;
    }

    @VisibleForTesting
    void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }

    @VisibleForTesting
    void setRecommendationService(RecommendationService recommendationService) {
        this.recommendationService = recommendationService;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setDanteLeadProxy(DanteLeadProxy danteLeadProxy) {
        this.danteLeadProxy = danteLeadProxy;
    }

    @VisibleForTesting
    void initLookupFieldsConfiguration() {
        accountLookupFields = new HashMap<>();
        accountLookupFields.put(BusinessEntity.Account,
                Arrays.asList(InterfaceName.AccountId.name(), //
                        InterfaceName.LEAccountIDLong.name(), //
                        InterfaceName.SalesforceAccountID.name(), //
                        InterfaceName.CompanyName.name(), //
                        InterfaceName.LDC_Name.name()));

        contactLookupFields = new HashMap<>();
        contactLookupFields.put(BusinessEntity.Account, Arrays.asList(InterfaceName.AccountId.name()));
        contactLookupFields.put(BusinessEntity.Contact,
                Arrays.asList(InterfaceName.AccountId.name(), //
                        InterfaceName.ContactId.name(), //
                        InterfaceName.CompanyName.name(), //
                        InterfaceName.Email.name(), //
                        InterfaceName.ContactName.name(), //
                        InterfaceName.City.name(), //
                        InterfaceName.State.name(), //
                        InterfaceName.Country.name(), //
                        InterfaceName.PostalCode.name(), //
                        InterfaceName.PhoneNumber.name(), //
                        InterfaceName.Title.name(), //
                        InterfaceName.Address_Street_1.name()));
    }
}
