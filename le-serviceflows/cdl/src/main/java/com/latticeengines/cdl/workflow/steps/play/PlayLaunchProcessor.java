package com.latticeengines.cdl.workflow.steps.play;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

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
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.Counter;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
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
    private SqoopProxy sqoopProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobService jobService;

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

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public void executeLaunchActivity(Tenant tenant, PlayLaunchInitStepConfiguration config) throws IOException {
        // initialize play launch context
        PlayLaunchContext playLaunchContext = initPlayLaunchContext(tenant, config);

        // prepare account and contact front end queries
        frontEndQueryCreator.prepareFrontEndQueries(playLaunchContext);

        long segmentAccountsCount = accountFetcher.getCount(playLaunchContext);
        log.info(String.format("Total records in segment: %d", segmentAccountsCount));
        playLaunchContext.getPlayLaunch().setAccountsSelected(segmentAccountsCount);

        // do initial handling of SFDC id based suppression
        handleSFDCIdBasedSuppression(playLaunchContext, segmentAccountsCount);

        Long currentTimeMillis = System.currentTimeMillis();

        String avroFileName = playLaunchContext.getPlayLaunchId() + ".avro";

        File localFile = new File(tenant.getName() + "_" + currentTimeMillis + "_" + avroFileName);

        if (segmentAccountsCount > 0) {
            // process accounts that exists in segment
            long processedSegmentAccountsCount = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);
            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>(playLaunchContext.getSchema()))) {
                dataFileWriter.create(playLaunchContext.getSchema(), localFile);

                // loop over to required number of pages
                for (int pageNo = 0; pageNo < pages; pageNo++) {
                    // fetch and process a single page
                    processedSegmentAccountsCount = fetchAndProcessPage(playLaunchContext, segmentAccountsCount,
                            processedSegmentAccountsCount, pageNo, dataFileWriter);
                }
            }

        }

        // as per PM requirement, we need to fail play launch if 0
        // recommendations were created. In future this may be enhanced for
        // %based failure rate to decide if play launch failed or not
        if (playLaunchContext.getPlayLaunch().getAccountsLaunched() == null
                || playLaunchContext.getPlayLaunch().getAccountsLaunched().longValue() == 0L) {
            throw new LedpException(LedpCode.LEDP_18159,
                    new Object[] { playLaunchContext.getPlayLaunch().getAccountsLaunched(),
                            playLaunchContext.getPlayLaunch().getAccountsErrored() });
        } else {
            runSqoopExportRecommendations(tenant, playLaunchContext, currentTimeMillis, avroFileName, localFile);
        }
    }

    private void runSqoopExportRecommendations(Tenant tenant, PlayLaunchContext playLaunchContext,
            Long currentTimeMillis, String avroFileName, File localFile) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        String path = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        path = path.endsWith("/") ? path : path + "/";
        path += currentTimeMillis + "/";

        String avroPath = path + "avro/";

        try {
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroPath + avroFileName);
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

        try {
            HdfsUtils.rmdir(yarnConfiguration, //
                    avroPath.substring(0, avroPath.lastIndexOf("/avro")));
        } catch (Exception ex) {
            log.error(
                    "Ignoring error while deleting dir: " //
                            + avroPath.substring(0, avroPath.lastIndexOf("/avro")), //
                    ex);
        }
    }

    private boolean export(PlayLaunchContext playLaunchContext, String avroPath) {
        String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
        String tenant = playLaunchContext.getTenant().getId();

        log.info("Trying to submit sqoop job for exporting recommendations from " + avroPath);
        DbCreds.Builder credsBldr = new DbCreds.Builder();
        credsBldr.user(dataDbUser).dbType(dataDbType).driverClass(dataDbDriver)
                .jdbcUrl(dataDbUrl + "?user=" + dataDbUser + "&password=" + dataDbPassword);

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
                .waitFinalJobStatus(appId, (new Long(TimeUnit.MINUTES.toSeconds(60L))).intValue()).getStatus();

        log.info("Sqoop job final status: " + sqoopJobStatus.name());

        return sqoopJobStatus == FinalApplicationStatus.SUCCEEDED;
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
            long processedSegmentAccountsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter) {
        log.info(String.format("Loop #%d", pageNo));

        // fetch accounts in current page
        DataPage accountsPage = //
                accountFetcher.fetch(//
                        playLaunchContext, segmentAccountsCount, processedSegmentAccountsCount);

        // process accounts in current page
        processedSegmentAccountsCount += processAccountsPage(playLaunchContext, accountsPage, dataFileWriter);

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

        Table recommendationTable = metadataProxy.getTable(tenant.getId(), playLaunch.getTableName());

        Schema schema = TableUtils.createSchema(playLaunch.getTableName(), recommendationTable);

        ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace.getTenantId(), ratingEngine.getId());
        RatingModel activeModel = ratingEngine.getActiveModel();
        String modelId = activeModel.getId();
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
                .activeModel(activeModel) //
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
        playLaunch.setAccountsSuppressed(playLaunchContext.getCounter().getAccountSuppressed().get());

        updateLaunchProgress(playLaunchContext);
        log.info("launch progress: " + playLaunch.getLaunchCompletionPercent() + "% completed");
    }

    private long processAccountsPage(PlayLaunchContext playLaunchContext, DataPage accountsPage,
            DataFileWriter<GenericRecord> dataFileWriter) {
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
                    accountList, mapForAccountAndContactList, dataFileWriter);
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
