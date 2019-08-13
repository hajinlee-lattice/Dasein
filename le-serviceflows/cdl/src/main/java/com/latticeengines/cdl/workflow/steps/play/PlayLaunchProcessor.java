package com.latticeengines.cdl.workflow.steps.play;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.Counter;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("playLaunchProcessor")
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

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobService jobService;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Value("${datadb.datasource.dialect}")
    private String dataDbDialect;

    @Value("${datadb.datasource.type}")
    private String dataDbType;

    // NOTE - do not increase this pagesize beyond 2.5K as it causes failure in
    // count query for corresponding counts. Also do not increase it beyond 250
    // otherwise contact fetch time increases significantly. After lot of trial
    // and error we found 150 to be a good number
    @Value("${playmaker.workflow.segment.pagesize:150}")
    private long pageSize;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private BatonService batonService;

    public String launchPlay(Tenant tenant, PlayLaunchInitStepConfiguration config) throws IOException {
        // initialize play launch context
        PlayLaunchContext playLaunchContext = initPlayLaunchContext(tenant, config);
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();

        try {
            long totalAccountsAvailableForLaunch = playLaunch.getAccountsSelected();
            long totalContactsAvailableForLaunch = playLaunch.getContactsSelected();
            log.info(String.format("Total available accounts available for Launch: %d",
                    totalAccountsAvailableForLaunch));

            DataCollection.Version version = dataCollectionProxy
                    .getActiveVersion(playLaunchContext.getCustomerSpace().toString());
            log.info(String.format("Using DataCollection.Version %s", version));

            long totalAccountsCount = prepareQueriesAndCalculateAccCountForLaunch(playLaunchContext, version);

            Long currentTimeMillis = System.currentTimeMillis();

            String avroFileName = String.format("%s.avro", playLaunchContext.getPlayLaunchId());

            File localFile = new File(String.format("%s_%s_%s", tenant.getName(), currentTimeMillis, avroFileName));

            boolean entityMatchEnabled = Boolean.TRUE == batonService
                    .isEntityMatchEnabled(playLaunchContext.getCustomerSpace());

            if (totalAccountsCount > 0) {
                // process accounts that exists in segment
                long processedSegmentAccountsCount = 0;
                // find total number of pages needed
                int pages = (int) Math.ceil((totalAccountsCount * 1.0D) / pageSize);
                log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

                try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                        new GenericDatumWriter<>(playLaunchContext.getSchema()))) {
                    dataFileWriter.create(playLaunchContext.getSchema(), localFile);

                    // loop over to required number of pages
                    for (int pageNo = 0; pageNo < pages; pageNo++) {
                        // fetch and process a single page
                        processedSegmentAccountsCount = fetchAndProcessPage(playLaunchContext, totalAccountsCount,
                                processedSegmentAccountsCount, pageNo, dataFileWriter, version, entityMatchEnabled);
                    }
                }

            }

            // as per PM requirement, we need to fail play launch if 0
            // recommendations were created. In future this may be enhanced for
            // %based failure rate to decide if play launch failed or not
            if (playLaunch.getAccountsLaunched() == null || playLaunch.getAccountsLaunched() == 0L) {
                throw new LedpException(LedpCode.LEDP_18159,
                        new Object[] { playLaunch.getAccountsLaunched(), playLaunch.getAccountsErrored() });
            } else {
                String recAvroHdfsFilePath = runSqoopExportRecommendations(tenant, playLaunchContext, currentTimeMillis,
                        avroFileName, localFile);
                long suppressedAccounts = (totalAccountsAvailableForLaunch - playLaunch.getAccountsLaunched()
                        - playLaunch.getAccountsErrored());
                playLaunch.setAccountsSuppressed(suppressedAccounts);
                long suppressedContacts = (totalContactsAvailableForLaunch - playLaunch.getContactsLaunched()
                        - playLaunch.getContactsErrored());
                playLaunch.setContactsSuppressed(suppressedContacts);
                updateLaunchProgress(playLaunchContext);
                log.info(String.format("Total launched accounts count: %d", playLaunch.getAccountsLaunched()));
                log.info(String.format("Total errored accounts count: %d", playLaunch.getAccountsErrored()));
                log.info(String.format("Total suppressed account count for launch: %d", suppressedAccounts));
                return recAvroHdfsFilePath;
            }
        } catch (Exception ex) {
            log.info(String.format("Setting all counts to 0L as we encountered critical exception: %s",
                    ex.getMessage()));

            playLaunch.setAccountsSuppressed(0L);
            playLaunch.setAccountsErrored(0L);
            playLaunch.setAccountsLaunched(0L);
            playLaunch.setContactsLaunched(0L);
            updateLaunchProgress(playLaunchContext);

            throw ex;
        }
    }

    private long prepareQueriesAndCalculateAccCountForLaunch(PlayLaunchContext playLaunchContext,
            DataCollection.Version version) {
        long totalAccountsCount = handleBasicConfigurationAndBucketSelection(playLaunchContext, version);

        applyEmailFilterToQueries(playLaunchContext);

        totalAccountsCount = handleLookupIdBasedSuppression(playLaunchContext, totalAccountsCount, version);

        totalAccountsCount = handleTopNLimit(playLaunchContext, totalAccountsCount);

        log.info(String.format("Total accounts count for launch: %d", totalAccountsCount));
        return totalAccountsCount;
    }

    private long handleBasicConfigurationAndBucketSelection(PlayLaunchContext playLaunchContext,
            DataCollection.Version version) {
        // prepare account and contact front end queries
        frontEndQueryCreator.prepareFrontEndQueries(playLaunchContext);
        return accountFetcher.getCount(playLaunchContext, version);
    }

    private long handleTopNLimit(PlayLaunchContext playLaunchContext, long totalAccountsCount) {
        Long topNCount = playLaunchContext.getPlayLaunch().getTopNCount();

        if (topNCount != null) {
            log.info(String.format("Handling Top N Count: %d", topNCount));
            // if top n count is smaller than count in segment then update the
            // count for launch
            if (topNCount < totalAccountsCount) {
                totalAccountsCount = topNCount;
            }
        }
        return totalAccountsCount;
    }

    private String runSqoopExportRecommendations(Tenant tenant, PlayLaunchContext playLaunchContext,
            Long currentTimeMillis, String avroFileName, File localFile) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        String path = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        path = path.endsWith("/") ? path : path + "/";
        path += currentTimeMillis + "/";

        String avroPath = path + "avro/";
        String recAvroHdfsFilePath = avroPath + avroFileName;

        try {
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), recAvroHdfsFilePath);
        } finally {
            FileUtils.deleteQuietly(localFile);
        }

        Extract extract = new Extract();
        extract.setExtractionTimestamp(currentTimeMillis);
        extract.setName("recommendationGeneration");
        extract.setPath(avroPath + "*.avro");
        extract.setTable(playLaunchContext.getRecommendationTable());
        extract.setTenant(tenant);
        playLaunchContext.getRecommendationTable().addExtract(extract);
        metadataProxy.updateTable(tenant.getId(), playLaunchContext.getPlayLaunch().getTableName(),
                playLaunchContext.getRecommendationTable());

        if (!export(playLaunchContext, avroPath)) {
            throw new LedpException(LedpCode.LEDP_18168);
        }

        return recAvroHdfsFilePath;
    }

    private boolean export(PlayLaunchContext playLaunchContext, String avroPath) {
        String queue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
        String tenant = playLaunchContext.getTenant().getId();

        log.info("Trying to submit sqoop job for exporting recommendations from " + avroPath);
        DbCreds.Builder credsBldr = new DbCreds.Builder();
        String connector = dataDbUrl.contains("?") ? "&" : "?";
        credsBldr.user(dataDbUser).dbType(dataDbType).driverClass(dataDbDriver)
                .jdbcUrl(dataDbUrl + connector + "user=" + dataDbUser + "&password=" + dataDbPassword);

        DbCreds dataDbCreds = new DbCreds(credsBldr);

        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setQueue(queue)//
                .setTable("Recommendation") //
                .setSourceDir(avroPath) //
                .setDbCreds(dataDbCreds) //
                .setCustomer(tenant) //
                .build();

        String appId = sqoopProxy.exportData(exporter).getApplicationIds().get(0);
        log.info("Submitted sqoop jobs: " + appId);

        FinalApplicationStatus sqoopJobStatus = jobService
                .waitFinalJobStatus(appId, (Long.valueOf(TimeUnit.MINUTES.toSeconds(60L))).intValue()).getStatus();

        log.info("Sqoop job final status: " + sqoopJobStatus.name());

        return sqoopJobStatus == FinalApplicationStatus.SUCCEEDED;
    }

    private long handleLookupIdBasedSuppression(PlayLaunchContext playLaunchContext, long totalAccountsCount,
            DataCollection.Version version) {
        // do handling of SFDC id based suppression

        long effectiveAccountCount = totalAccountsCount;
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        if (launch.getExcludeItemsWithoutSalesforceId()) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();

            Restriction accountRestriction = accountFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder()
                    .let(BusinessEntity.Account, launch.getDestinationAccountId()).isNotNull().build();

            Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                    .and(accountRestriction, nonNullLookupIdRestriction).build();
            accountFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);

            effectiveAccountCount = accountFetcher.getCount(playLaunchContext, version);
        }

        return effectiveAccountCount;
    }

    private void applyEmailFilterToQueries(PlayLaunchContext playLaunchContext) {
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        LookupIdMap lookupIdMap = lookupIdMappingProxy.getLookupIdMapByOrgId(playLaunchContext.getTenant().getId(),
                launch.getDestinationOrgId(), launch.getDestinationSysType());
        CDLExternalSystemName destinationSystemName = lookupIdMap.getExternalSystemName();
        if (CDLExternalSystemName.Marketo.equals(destinationSystemName)) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
            Restriction newContactRestrictionForAccountQuery = applyEmailFilterToContactRestriction(
                    accountFrontEndQuery.getContactRestriction().getRestriction());
            accountFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestrictionForAccountQuery));

            FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();
            Restriction newContactRestrictionForContactQuery = applyEmailFilterToContactRestriction(
                    contactFrontEndQuery.getContactRestriction().getRestriction());
            contactFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestrictionForContactQuery));
        }
    }

    private Restriction applyEmailFilterToContactRestriction(Restriction contactRestriction) {
        Restriction emailFilter = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Email.name())
                .isNotNull().build();
        return Restriction.builder().and(contactRestriction, emailFilter).build();
    }

    private long fetchAndProcessPage(PlayLaunchContext playLaunchContext, long segmentAccountsCount,
            long processedSegmentAccountsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter,
            DataCollection.Version version, boolean entityMatchEnabled) {
        log.info(String.format("Loop #%d", pageNo));

        // fetch accounts in current page
        DataPage accountsPage = //
                accountFetcher.fetch(//
                        playLaunchContext, segmentAccountsCount, processedSegmentAccountsCount, version);

        // process accounts in current page
        processedSegmentAccountsCount += processAccountsPage(playLaunchContext, accountsPage, dataFileWriter, version,
                entityMatchEnabled);

        // update launch progress
        updateLaunchProgress(playLaunchContext, processedSegmentAccountsCount, segmentAccountsCount);
        return processedSegmentAccountsCount;
    }

    private PlayLaunchContext initPlayLaunchContext(Tenant tenant, PlayLaunchInitStepConfiguration config) {
        PlayLaunchContextBuilder playLaunchContextBuilder = new PlayLaunchContextBuilder();

        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Play play = playProxy.getPlay(customerSpace.toString(), playName);
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.toString(), playName, playLaunchId);
        playLaunch.setPlay(play);
        long launchTimestampMillis = playLaunch.getCreated().getTime();

        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine != null) {
            ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace.getTenantId(), ratingEngine.getId());
        }

        MetadataSegment segment;
        if (play.getTargetSegment() != null) {
            segment = play.getTargetSegment();
        } else {
            log.info(String.format(
                    "No Target segment defined for Play %s, falling back to target segment of Rating Engine %s",
                    play.getName(), ratingEngine.getSegment()));
            if (ratingEngine != null) {
                segment = play.getRatingEngine().getSegment();
            }
            segment = null;
        }

        if (segment == null) {
            throw new NullPointerException(String.format("No Target segment defined for Play %s or Rating Engine %s",
                    play.getName(), ratingEngine.getId()));
        }

        String segmentName = segment.getName();

        Table recommendationTable = metadataProxy.getTable(tenant.getId(), playLaunch.getTableName());
        Schema schema = TableUtils.createSchema(playLaunch.getTableName(), recommendationTable);
        RatingModel publishedIteration = null;
        String modelId = null;
        String ratingId = null;
        if (ratingEngine != null) {
            publishedIteration = ratingEngine.getPublishedIteration();
            modelId = publishedIteration.getId();
            ratingId = ratingEngine.getId();
        }
        log.info(String.format("Processing segment: %s", segmentName));

        playLaunchContextBuilder //
                .customerSpace(customerSpace) //
                .tenant(tenant) //
                .launchTimestampMillis(launchTimestampMillis) //
                .playName(playName) //
                .play(play) //
                .playLaunchId(playLaunchId) //
                .playLaunch(playLaunch) //
                .ratingId(ratingId) //
                .ratingEngine(ratingEngine) //
                .publishedIterationId(modelId) //
                .publishedIteration(publishedIteration) //
                .segmentName(segmentName) //
                .segment(segment) //
                .accountFrontEndQuery(new FrontEndQuery()) //
                .contactFrontEndQuery(new FrontEndQuery()) //
                .modifiableAccountIdCollectionForContacts(new ArrayList<>()) //
                .counter(new Counter()) //
                .recommendationTable(recommendationTable) //
                .schema(schema);

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

    private long processAccountsPage(PlayLaunchContext playLaunchContext, DataPage accountsPage,
            DataFileWriter<GenericRecord> dataFileWriter, DataCollection.Version version, boolean entityMatchEnabled) {
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
                    contactFetcher.fetch(playLaunchContext, version);

            // generate recommendations using list of accounts in page and
            // corresponding account/contacts map
            recommendationCreator.generateRecommendations(playLaunchContext, //
                    accountList, mapForAccountAndContactList, dataFileWriter, entityMatchEnabled);
        }
        return accountList.size();
    }

    private void updateLaunchProgress(PlayLaunchContext playLaunchContext) {
        try {
            PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();

            playProxy.updatePlayLaunchProgress(playLaunchContext.getCustomerSpace().toString(), //
                    playLaunch.getPlay().getName(), playLaunch.getLaunchId(), playLaunch.getLaunchCompletionPercent(),
                    playLaunch.getAccountsLaunched(), playLaunch.getContactsLaunched(), playLaunch.getAccountsErrored(),
                    playLaunch.getAccountsSuppressed(), playLaunch.getContactsSuppressed(),
                    playLaunch.getContactsErrored());
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
    void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

    @VisibleForTesting
    void setLookupIdMappingProxy(LookupIdMappingProxy lookupIdMappingProxy) {
        this.lookupIdMappingProxy = lookupIdMappingProxy;
    }

    @VisibleForTesting
    void setMetadataProxy(MetadataProxy metadataProxy) {
        this.metadataProxy = metadataProxy;
    }

    @VisibleForTesting
    void setSqoopProxy(SqoopProxy sqoopProxy) {
        this.sqoopProxy = sqoopProxy;
    }

    @VisibleForTesting
    void setRatingEngineProxy(RatingEngineProxy ratingEngineProxy) {
        this.ratingEngineProxy = ratingEngineProxy;
    }

    @VisibleForTesting
    void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    @VisibleForTesting
    void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

    @VisibleForTesting
    void setDataDbDriver(String dataDbDriver) {
        this.dataDbDriver = dataDbDriver;
    }

    @VisibleForTesting
    void setDataDbUrl(String dataDbUrl) {
        this.dataDbUrl = dataDbUrl;
    }

    @VisibleForTesting
    void setDataDbUser(String dataDbUser) {
        this.dataDbUser = dataDbUser;
    }

    @VisibleForTesting
    void setDataDbPassword(String dataDbPassword) {
        this.dataDbPassword = dataDbPassword;
    }

    @VisibleForTesting
    void setDataDbDialect(String dataDbDialect) {
        this.dataDbDialect = dataDbDialect;
    }

    @VisibleForTesting
    void setDataDbType(String dataDbType) {
        this.dataDbType = dataDbType;
    }

}
