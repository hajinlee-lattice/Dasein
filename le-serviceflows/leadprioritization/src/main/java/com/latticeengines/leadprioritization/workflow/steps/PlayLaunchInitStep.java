package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
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
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

    private static final String NAME_AS_PER_LATTICE_MATCH = "LDC_Name";

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
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DanteLeadProxy danteLeadProxy;

    private long launchTimestampMillis;

    private long segmentAccountsCount = 0;

    private long processedSegmentAccountsCount = 0;

    List<String> fields = Arrays.asList(InterfaceName.AccountId.name(), //
            InterfaceName.SalesforceAccountID.name(), //
            InterfaceName.CompanyName.name(), //
            NAME_AS_PER_LATTICE_MATCH);

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
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

            String segmentName = play.getSegmentName();
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

        List<Lookup> lookups = fields.stream().map(fieldName -> new AttributeLookup(BusinessEntity.Account, fieldName))
                .collect(Collectors.toList());

        accountFrontEndQuery.setLookups(lookups);
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

    private void prepareQueryUsingSegmentDefn(CustomerSpace customerSpace, String segmentName,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts) throws JsonProcessingException {
        Restriction segmentRestriction = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                segmentName);

        log.info(String.format("Processing restriction: %s", objectMapper.writeValueAsString(segmentRestriction)));

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(segmentRestriction);
        accountFrontEndQuery.setAccountRestriction(frontEndRestriction);
        contactFrontEndQuery.setAccountRestriction(prepareContactRestriction(modifiableAccountIdCollectionForContacts));
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

            contactFrontEndQuery
                    .setAccountRestriction(prepareContactRestriction(modifiableAccountIdCollectionForContacts));
            contactFrontEndQuery.setContactRestriction(contactRestriction);
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

    private FrontEndRestriction prepareContactRestriction(Collection<Object> modifiableAccountIdCollection) {
        FrontEndRestriction wrapperFrontEndRestriction = new FrontEndRestriction();
        AttributeLookup accountIdLookup = new AttributeLookup();
        accountIdLookup.setEntity(BusinessEntity.Account);
        accountIdLookup.setAttribute(InterfaceName.AccountId.name());
        CollectionLookup accountIdListLookup = new CollectionLookup(modifiableAccountIdCollection);
        ConcreteRestriction collectionRestriction = new ConcreteRestriction(false, accountIdLookup,
                ComparisonType.IN_COLLECTION, accountIdListLookup);

        wrapperFrontEndRestriction.setRestriction(collectionRestriction);
        return wrapperFrontEndRestriction;
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

                DataPage accountPage = fetchAccounts(config, accountFrontEndQuery);

                processAccounts(tenant, playLaunch, config, contactFrontEndQuery, //
                        modifiableAccountIdCollectionForContacts, modelId, //
                        accountLaunched, contactLaunched, accountErrored, accountSuppressed, accountPage);

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

    private DataPage fetchAccounts(PlayLaunchInitStepConfiguration config, FrontEndQuery accountFrontEndQuery) {
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
            AtomicLong accountSuppressed, DataPage accountPage) {
        List<Map<String, Object>> accountList = accountPage.getData();

        if (CollectionUtils.isNotEmpty(accountList)) {

            List<Long> accountIds = getAccountsIds(accountList);

            modifiableAccountIdCollectionForContacts.clear();
            modifiableAccountIdCollectionForContacts.addAll(accountIds);
            Map<Long, List<Map<String, String>>> mapForAccountAndContactList = new HashMap<>();

            fetchContacts(config, contactFrontEndQuery, mapForAccountAndContactList);

            generateRecommendations(tenant, playLaunch, config, modelId, //
                    accountLaunched, contactLaunched, accountErrored, accountSuppressed, accountList, //
                    mapForAccountAndContactList);
        }
        processedSegmentAccountsCount += accountList.size();
    }

    private List<Long> getAccountsIds(List<Map<String, Object>> accountList) {
        List<Long> accountIds = //
                parallelStream(accountList) //
                        .map(account -> //
                        {
                            Object accountIdObj = account.get(InterfaceName.AccountId.name());
                            if (accountIdObj instanceof Long) {
                                return (Long) accountIdObj;
                            }
                            return accountIdObj != null && StringUtils.isNotBlank(accountIdObj.toString())
                                    && StringUtils.isNumeric(accountIdObj.toString())
                                            ? Long.parseLong(accountIdObj.toString()) : null;
                        })//
                        .collect(Collectors.toList());

        log.info(String.format("Extracting contacts for accountIds: %s",
                Arrays.deepToString(accountIds.toArray(new Long[accountIds.size()]))));
        return accountIds;
    }

    private void fetchContacts(PlayLaunchInitStepConfiguration config, FrontEndQuery contactFrontEndQuery,
            Map<Long, List<Map<String, String>>> mapForAccountAndContactList) {
        try {
            log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));
            DataPage contactPage = entityProxy.getData( //
                    config.getCustomerSpace().toString(), //
                    contactFrontEndQuery);

            log.info(String.format("Got # %d contact elements in this loop", contactPage.getData().size()));
            contactPage.getData().stream().forEach(contact -> {
                Object accountIdObj = contact.get(InterfaceName.AccountId.name());
                Long accountId = parseLong(accountIdObj);

                if (accountId != null) {
                    if (!mapForAccountAndContactList.containsKey(accountId)) {
                        mapForAccountAndContactList.put(accountId, new ArrayList<>());
                    }
                    List<Map<String, String>> contacts = mapForAccountAndContactList.get(accountId);
                    contacts.add(convertValuesToString(contact));
                }
            });

        } catch (Exception ex) {
            log.error("Ignoring till contact data is available in cdl", ex);
        }
    }

    private void generateRecommendations(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            String modelId, AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, List<Map<String, Object>> accountList,
            Map<Long, List<Map<String, String>>> mapForAccountAndContactList) {
        parallelStream(accountList) //
                .forEach(account -> {
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
            AtomicLong accountSuppressed, Map<Long, List<Map<String, String>>> mapForAccountAndContactList,
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

    private Long parseLong(Object accountIdObj) {
        Long accountId = null;
        if (accountIdObj != null) {
            if (accountIdObj instanceof Long) {
                accountId = (Long) accountIdObj;
            } else {
                String accountIdStr = accountIdObj.toString();
                if (StringUtils.isNotBlank(accountIdStr) && StringUtils.isNumeric(accountIdStr)) {
                    accountId = Long.parseLong(accountIdStr);
                }
            }
        }
        return accountId;
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
            Map<Long, List<Map<String, String>>> mapForAccountAndContactList, String modelId) {
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(checkAndGet(account, InterfaceName.AccountId.name()));
        recommendation.setLeAccountExternalID(checkAndGet(account, InterfaceName.AccountId.name()));
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID.name()));
        Double value = 0D;
        recommendation.setMonetaryValue(value);

        // give preference to company name from customer data itself. If not
        // found then try to get company name from lattice data cloud field
        // LDC_Name
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        if (recommendation.getCompanyName() == null) {
            recommendation.setCompanyName(checkAndGet(account, NAME_AS_PER_LATTICE_MATCH));
        }

        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(0.5D);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);

        String bucketName = checkAndGet(account, modelId, "A");
        RuleBucketName bucket = null;
        bucket = RuleBucketName.getRuleBucketName(bucketName);

        if (bucket == null) {
            bucket = RuleBucketName.valueOf(bucketName);
        }

        recommendation.setPriorityID(bucket);
        recommendation.setPriorityDisplayName(bucket.getName());

        Long accountId = parseLong(recommendation.getAccountId());

        if (mapForAccountAndContactList.containsKey(accountId)) {
            recommendation.setExpandedContacts(mapForAccountAndContactList.get(accountId));
        } else {
            // TODO - read contact data from redshift api
            List<Map<String, String>> contactList = PlaymakerUtils
                    .createDummyContacts(StringUtils.isBlank(recommendation.getCompanyName()) ? "Dummy Company"
                            : recommendation.getCompanyName());
            recommendation.setExpandedContacts(contactList);
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

    private List<String> getSchema(Tenant tenant, TableRoleInCollection role) {
        List<Attribute> schemaAttributes = getSchemaAttributes(tenant, role);

        Stream<String> stream = schemaAttributes.stream() //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId)) //
                .map(ColumnMetadata::getColumnId);

        return stream.collect(Collectors.toList());
    }

    private List<Attribute> getSchemaAttributes(Tenant tenant, TableRoleInCollection role) {
        String customerSpace = tenant.getId();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        return schemaTable.getAttributes();
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
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

    @VisibleForTesting
    void setDanteLeadProxy(DanteLeadProxy danteLeadProxy) {
        this.danteLeadProxy = danteLeadProxy;
    }
}
