package com.latticeengines.leadprioritization.workflow.steps;

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
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
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

    private long processedSegmentAccountsCount = 0;

    List<String> fields = Arrays.asList(InterfaceName.AccountId.name(), //
            InterfaceName.SalesforceAccountID.name(), //
            InterfaceName.CompanyName.name());

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
            Restriction segmentRestriction = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                    segmentName);

            log.info("Processing restriction: " + objectMapper.writeValueAsString(segmentRestriction));
            FrontEndQuery frontEndQuery = new FrontEndQuery();
            frontEndQuery.setAccountRestriction(new FrontEndRestriction(segmentRestriction));
            frontEndQuery.setRestrictNotNullSalesforceId(play.getExcludeItemsWithoutSalesforceId());
            frontEndQuery.addLookups(BusinessEntity.Account, fields.toArray(new String[fields.size()]));
            frontEndQuery.setMainEntity(BusinessEntity.Account);

            executeLaunchActivity(tenant, playLaunch, config, frontEndQuery);

            internalResourceRestApiProxy.publishTalkingPoints(customerSpace, playName);
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
        }
    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
                                       FrontEndQuery frontEndQuery) {
        long totalAccounts = entityProxy.getCount(config.getCustomerSpace().toString(), frontEndQuery);
        log.info("Total records in segment: " + totalAccounts);

        long suppressedAccounts = 0;
        if (frontEndQuery.restrictNotNullSalesforceId()) {
            frontEndQuery.setRestrictNotNullSalesforceId(false);
            suppressedAccounts = entityProxy.getCount(config.getCustomerSpace().toString(), frontEndQuery) - totalAccounts;
            frontEndQuery.setRestrictNotNullSalesforceId(true);
        }
        playLaunch.setAccountsSuppressed(suppressedAccounts);

        if (totalAccounts > 0) {
            List<String> accountSchema = getSchema(tenant, TableRoleInCollection.BucketedAccount);
            DataRequest dataRequest = new DataRequest();
            dataRequest.setAttributes(accountSchema);
            AtomicLong launched = new AtomicLong();
            AtomicLong errored = new AtomicLong();

            int pages = (int) Math.ceil((totalAccounts * 1.0D) / pageSize);

            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            for (int pageNo = 0; pageNo < pages; pageNo++) {
                log.info("Loop #" + pageNo);

                long expectedPageSize = Math.min(pageSize, (totalAccounts - processedSegmentAccountsCount));

                frontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));
                frontEndQuery.setMainEntity(BusinessEntity.Account);

                DataPage accountPage = entityProxy.getData(config.getCustomerSpace().toString(), frontEndQuery);

                log.info("Got #" + accountPage.getData().size() + " elements in this loop");

                List<Map<String, Object>> accountList = accountPage.getData();

                if (CollectionUtils.isNotEmpty(accountList)) {

                    Stream<Map<String, Object>> parallelStream = //
                            accountList.stream() //
                                    .parallel();

                    parallelStream //
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
                playLaunch.setLaunchCompletionPercent(100 * processedSegmentAccountsCount / totalAccounts);
                playLaunch.setAccountsLaunched(launched.get());
                playLaunch.setAccountsErrored(errored.get());
                updateLaunchProgress(playLaunch);
            }
        }
    }

    private void updateLaunchProgress(PlayLaunch playLaunch) {
        try {
            internalResourceRestApiProxy.updatePlayLaunchProgress(getConfiguration().getCustomerSpace().toString(), //
                    playLaunch.getPlay().getName(),
                    playLaunch.getLaunchId(),
                    playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsLaunched(),
                    playLaunch.getAccountsErrored(),
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

        recommendation.setAccountId(checkAndGet(account, InterfaceName.AccountId));
        recommendation.setLeAccountExternalID(checkAndGet(account, InterfaceName.AccountId));
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID));
        Double value = 0D;
        recommendation.setMonetaryValue(value);
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName));
        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(0.5D);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);
        recommendation.setPriorityDisplayName("A");

        // TODO - read contact data from redshift api
        List<Map<String, String>> contactList = PlaymakerUtils
                .createDummyContacts(StringUtils.isBlank(recommendation.getCompanyName()) ? "Dummy Company"
                        : recommendation.getCompanyName());
        recommendation.setExpandedContacts(contactList);

        return recommendation;
    }

    private String checkAndGet(Map<String, Object> account, InterfaceName columnName) {
        return account.get(columnName.name()) != null ? account.get(columnName.name()).toString() : null;
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
}
