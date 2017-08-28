package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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

    private static final String SCORING_FIELD_NAME = "__RuleModel_Score__";

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

            log.info("For tenant: " + customerSpace.toString());
            log.info("For playId: " + playName);
            log.info("For playLaunchId: " + playLaunchId);

            PlayLaunch playLaunch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
            Play play = internalResourceRestApiProxy.findPlayByName(customerSpace, playName);
            playLaunch.setPlay(play);

            String segmentName = play.getSegmentName();
            log.info("Processing segment: " + segmentName);
            FrontEndQuery accountFrontEndQuery = new FrontEndQuery();
            FrontEndQuery contactFrontEndQuery = new FrontEndQuery();

            List<Lookup> lookups = new ArrayList<>();
            for (String fieldName : fields) {
                Lookup lookup = new AttributeLookup(BusinessEntity.Account, fieldName);
                lookups.add(lookup);
            }
            accountFrontEndQuery.setLookups(lookups);
            RatingEngine ratingEngine = play.getRatingEngine();
            if (ratingEngine != null) {
                if (ratingEngine.getSegment() != null) {
                    FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                            ratingEngine.getSegment().getRestriction());
                    accountFrontEndQuery.setAccountRestriction(frontEndRestriction);
                }
                accountFrontEndQuery.setMainEntity(BusinessEntity.Account);

                List<RatingModel> ratingModels = new ArrayList<>();
                for (RatingModel model : ratingEngine.getRatingModels()) {
                    ratingModels.add((RuleBasedModel) model);
                    break;
                }
                accountFrontEndQuery.setRatingModels(ratingModels);

                lookups = accountFrontEndQuery.getLookups();
                Lookup lookup = new AttributeLookup(BusinessEntity.Rating, SCORING_FIELD_NAME);
                lookups.add(lookup);

                for (RatingModel ratingModel : accountFrontEndQuery.getRatingModels()) {
                    ratingModel.setId(SCORING_FIELD_NAME);
                }
            }

            if (accountFrontEndQuery.getAccountRestriction() == null) {

                Restriction segmentRestriction = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                        segmentName);

                log.info("Processing restriction: " + objectMapper.writeValueAsString(segmentRestriction));

                FrontEndRestriction frontEndRestriction = new FrontEndRestriction(segmentRestriction);
                accountFrontEndQuery.setAccountRestriction(frontEndRestriction);
            }

            accountFrontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
            executeLaunchActivity(tenant, playLaunch, config, accountFrontEndQuery, contactFrontEndQuery);

            internalResourceRestApiProxy.publishTalkingPoints(customerSpace, playName);
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
        }
    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery) {

        segmentAccountsCount = entityProxy.getCount( //
                config.getCustomerSpace().toString(), //
                accountFrontEndQuery);
        log.info("Total records in segment: " + segmentAccountsCount);

        long suppressedAccounts = 0;
        if (accountFrontEndQuery.restrictNotNullSalesforceId()) {
            accountFrontEndQuery.setRestrictNotNullSalesforceId(false);
            suppressedAccounts = entityProxy.getCount(config.getCustomerSpace().toString(), accountFrontEndQuery)
                    - segmentAccountsCount;
            accountFrontEndQuery.setRestrictNotNullSalesforceId(true);
        }
        playLaunch.setAccountsSuppressed(suppressedAccounts);

        if (segmentAccountsCount > 0) {
            AtomicLong launched = new AtomicLong();
            AtomicLong errored = new AtomicLong();

            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);

            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            for (int pageNo = 0; pageNo < pages; pageNo++) {
                log.info("Loop #" + pageNo);

                long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));

                accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));
                accountFrontEndQuery.setMainEntity(BusinessEntity.Account);

                DataPage accountPage = entityProxy.getData( //
                        config.getCustomerSpace().toString(), //
                        accountFrontEndQuery);

                log.info("Got #" + accountPage.getData().size() + " elements in this loop");

                List<Map<String, Object>> accountList = accountPage.getData();

                if (CollectionUtils.isNotEmpty(accountList)) {

                    List<Long> accountIds = //
                            parallelStream(accountList) //
                                    .map(account -> //
                                    {
                                        String accountIdStr = (String) account.get(InterfaceName.AccountId.name());
                                        return StringUtils.isNotBlank(accountIdStr)
                                                && StringUtils.isNumeric(accountIdStr) ? Long.parseLong(accountIdStr)
                                                        : null;
                                    })//
                                    .collect(Collectors.toList());

                    log.info("Extracting contacts for accountIds: " + accountIds.toArray(new Long[accountIds.size()]));

                    parallelStream(accountList) //
                            .map(account -> {
                                try {
                                    Recommendation recommendation = //
                                            prepareRecommendation(tenant, playLaunch, config, account);
                                    recommendationService.create(recommendation);
                                    try {
                                        danteLeadProxy.create(recommendation, config.getCustomerSpace().toString());
                                    } catch (Exception e) {
                                        log.error("Failed to publish lead to dante, ignoring", e);
                                    }
                                    launched.addAndGet(1);
                                    return recommendation.getRecommendationId();
                                } catch (Throwable th) {
                                    log.error(th.getMessage(), th);
                                    errored.addAndGet(1);
                                    throw th;
                                }
                            }) //
                            .collect(Collectors.toList());
                }
                processedSegmentAccountsCount += accountList.size();
                playLaunch.setLaunchCompletionPercent(100 * processedSegmentAccountsCount / segmentAccountsCount);
                playLaunch.setAccountsLaunched(launched.get());
                playLaunch.setAccountsErrored(errored.get());
                updateLaunchProgress(playLaunch);
            }
        }
    }

    private Stream<Map<String, Object>> parallelStream(List<Map<String, Object>> accountList) {
        return accountList.stream() //
                .parallel();
    }

    private void updateLaunchProgress(PlayLaunch playLaunch) {
        try {
            internalResourceRestApiProxy.updatePlayLaunchProgress(getConfiguration().getCustomerSpace().toString(), //
                    playLaunch.getPlay().getName(), playLaunch.getLaunchId(), playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsLaunched(), playLaunch.getAccountsErrored(),
                    playLaunch.getAccountsSuppressed());
        } catch (Exception e) {
            log.error("Unable to update launch progress.", e);
        }
    }

    private Recommendation prepareRecommendation(Tenant tenant, PlayLaunch playLauch,
            PlayLaunchInitStepConfiguration config, Map<String, Object> account) {
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLauch.getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLauch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(checkAndGet(account, InterfaceName.AccountId.name()));
        recommendation.setLeAccountExternalID(checkAndGet(account, InterfaceName.AccountId.name()));
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID.name()));
        Double value = 0D;
        recommendation.setMonetaryValue(value);
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(0.5D);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);
        recommendation.setPriorityDisplayName(checkAndGet(account, SCORING_FIELD_NAME, "A"));

        // TODO - read contact data from redshift api
        List<Map<String, String>> contactList = PlaymakerUtils
                .createDummyContacts(StringUtils.isBlank(recommendation.getCompanyName()) ? "Dummy Company"
                        : recommendation.getCompanyName());
        recommendation.setExpandedContacts(contactList);

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
