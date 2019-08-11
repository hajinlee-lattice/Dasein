package com.latticeengines.cdl.workflow.steps.play;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.Counter;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.ExportFieldMetadataProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("campaignLaunchProcessor")
public class CampaignLaunchProcessor {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchProcessor.class);

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

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private ExportFieldMetadataProxy exportFieldMetadataProxy;

    public ProcessedFieldMappingMetadata prepareFrontEndQueries(PlayLaunchContext playLaunchContext,
            DataCollection.Version version) {
        // prepare basic account and contact front end queries
        ProcessedFieldMappingMetadata result = frontEndQueryCreator.prepareFrontEndQueries(playLaunchContext, true);
        applyEmailFilterToQueries(playLaunchContext);
        handleLookupIdBasedSuppression(playLaunchContext);
        return result;
    }

    public void runSqoopExportRecommendations(Tenant tenant, PlayLaunchContext playLaunchContext,
            Long currentTimeMillis, String recAvroHdfsFilePath) throws IOException {

        String extractPath = PathUtils.toAvroGlob(recAvroHdfsFilePath);
        log.info("extractPath: " + extractPath);
        Extract extract = new Extract();
        extract.setExtractionTimestamp(currentTimeMillis);
        extract.setName("recommendationGeneration");
        extract.setPath(extractPath);
        extract.setTable(playLaunchContext.getRecommendationTable());
        extract.setTenant(tenant);
        playLaunchContext.getRecommendationTable().addExtract(extract);
        metadataProxy.updateTable(tenant.getId(), playLaunchContext.getPlayLaunch().getTableName(),
                playLaunchContext.getRecommendationTable());

        if (!export(playLaunchContext, recAvroHdfsFilePath)) {
            throw new LedpException(LedpCode.LEDP_18168);
        }
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
                .addHadoopArg("-Dmapreduce.job.running.map.limit=8") //
                .addHadoopArg("-Dmapreduce.tasktracker.map.tasks.maximum=8") //
                .build();

        log.info("exporter is " + JsonUtils.serialize(exporter));
        String appId = sqoopProxy.exportData(exporter).getApplicationIds().get(0);
        log.info("Submitted sqoop jobs: " + appId);

        FinalApplicationStatus sqoopJobStatus = jobService
                .waitFinalJobStatus(appId, (Long.valueOf(TimeUnit.MINUTES.toSeconds(60L))).intValue()).getStatus();

        log.info("Sqoop job final status: " + sqoopJobStatus.name());

        return sqoopJobStatus == FinalApplicationStatus.SUCCEEDED;
    }

    private void handleLookupIdBasedSuppression(PlayLaunchContext playLaunchContext) {
        // do handling of SFDC id based suppression
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        if (launch.getExcludeItemsWithoutSalesforceId()) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();

            Restriction accountRestriction = accountFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder()
                    .let(BusinessEntity.Account, launch.getDestinationAccountId()).isNotNull().build();

            Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                    .and(accountRestriction, nonNullLookupIdRestriction).build();
            accountFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);

        }
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

    public PlayLaunchContext initPlayLaunchContext(Tenant tenant, CampaignLaunchInitStepConfiguration config) {
        PlayLaunchContextBuilder playLaunchContextBuilder = new PlayLaunchContextBuilder();

        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Play play = playProxy.getPlay(customerSpace.toString(), playName);
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.toString(), playName, playLaunchId);
        List<ColumnMetadata> fieldMappingMetadata = null;
        PlayLaunchChannel playLaunchChannel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace.toString(),
                playName, playLaunchId);
        if (playLaunchChannel != null) {
            fieldMappingMetadata = exportFieldMetadataProxy.getExportFields(customerSpace.toString(),
                    playLaunchChannel.getId());
            playLaunch.setDestinationOrgName(playLaunchChannel.getLookupIdMap().getOrgName());
            log.info("For tenant= " + tenant.getName() + ", playLaunchId= " + playLaunchChannel.getId()
                    + ", the list of columnmetadata is:");
            log.info(Arrays.toString(fieldMappingMetadata.toArray()));
        }
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
                .fieldMappingMetadata(fieldMappingMetadata) //
                .counter(new Counter()) //
                .recommendationTable(recommendationTable) //
                .schema(schema);

        return playLaunchContextBuilder.build();
    }

    public void updateLaunchProgress(PlayLaunchContext playLaunchContext) {
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

    public static class ProcessedFieldMappingMetadata {

        private List<String> accountColsRecIncluded;

        private List<String> accountColsRecNotIncludedStd;

        private List<String> accountColsRecNotIncludedNonStd;

        private List<String> contactCols;

        public List<String> getAccountColsRecIncluded() {
            return this.accountColsRecIncluded;
        }

        public void setAccountColsRecIncluded(List<String> accountColsRecIncluded) {
            this.accountColsRecIncluded = accountColsRecIncluded;
        }

        public List<String> getAccountColsRecNotIncludedStd() {
            return this.accountColsRecNotIncludedStd;
        }

        public void setAccountColsRecNotIncludedStd(List<String> accountColsRecNotIncludedStd) {
            this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
        }

        public List<String> getAccountCoslRecNotIncludedNonStd() {
            return this.accountColsRecNotIncludedNonStd;
        }

        public void setAccountCoslRecNotIncludedNonStd(List<String> accountColsRecNotIncludedNonStd) {
            this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
        }

        public List<String> getContactCols() {
            return this.contactCols;
        }

        public void setContactCols(List<String> contactCols) {
            this.contactCols = contactCols;
        }

    }

}
