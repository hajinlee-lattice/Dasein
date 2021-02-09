package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.domain.exposed.pls.Play.TapType;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.cdl.workflow.steps.campaign.utils.CampaignFrontEndQueryBuilder;
import com.latticeengines.cdl.workflow.steps.campaign.utils.CampaignLaunchUtils;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLaunchUniverseStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.spark.exposed.job.cdl.GenerateLaunchUniverseJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@Component("generateLaunchUniverse")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateLaunchUniverse extends BaseSparkSQLStep<GenerateLaunchUniverseStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchUniverse.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private CampaignLaunchUtils campaignLaunchUtils;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;
    private boolean contactsDataExists;

    @Override
    public void execute() {
        GenerateLaunchUniverseStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        Play play = playProxy.getPlay(customerSpace.getTenantId(), config.getPlayId(), false, false);
        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), config.getPlayId(),
                config.getChannelId());
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Campaign found by ID: " + config.getPlayId() });
        }
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Channel found by ID: " + config.getChannelId() });
        }
        PlayLaunch launch = null;
        if (StringUtils.isNotBlank(config.getLaunchId())) {
            launch = playProxy.getPlayLaunch(customerSpace.getTenantId(), config.getPlayId(), config.getLaunchId());
        }
        ChannelConfig channelConfig = launch == null ? channel.getChannelConfig() : launch.getChannelConfig();
        BusinessEntity mainEntity = channelConfig.getAudienceType().asBusinessEntity();
        Long maxEntitiesToLaunch = channel.getMaxEntitiesToLaunch();
        Long maxContactsPerAccount = channel.getMaxContactsPerAccount();
        Long contactAccountRatioThreshold = WorkflowJobUtils.getContactAccountRatioThresholdFromZK(customerSpace);
        boolean useContactsPerAccountLimit = useContactPerAccountLimit(mainEntity, maxContactsPerAccount);
        boolean useSparkJobContactsPerAccount = useSparkJobContactsPerAccount(mainEntity, maxContactsPerAccount, contactAccountRatioThreshold);
        Set<RatingBucketName> launchBuckets = launch == null ? channel.getBucketsToLaunch()
                : launch.getBucketsToLaunch();
        String lookupId = launch == null ? channel.getLookupIdMap().getAccountId() : launch.getDestinationAccountId();
        boolean launchUnScored = launch == null ? channel.getLaunchUnscored() : launch.isLaunchUnscored();
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();
        HdfsDataUnit launchUniverseDataUnit;
        TapType tapType = play.getTapType();
        boolean baseOnOtherTapType = TapType.ListSegment.equals(tapType);
        if (baseOnOtherTapType) {
            DataUnit input;
            CopyConfig copyConfig = new CopyConfig();
            if (BusinessEntity.Account.equals(mainEntity)) {
                input = getObjectFromContext(ACCOUNTS_DATA_UNIT, HdfsDataUnit.class);
                copyConfig.setSelectAttrs(Lists.newArrayList(InterfaceName.AccountId.name()));
            } else {
                input = getObjectFromContext(CONTACTS_DATA_UNIT, HdfsDataUnit.class);
                copyConfig.setSelectAttrs(Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name()));
            }
            copyConfig.setInput(Collections.singletonList(input));
            copyConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            SparkJobResult sparkJobResult = runSparkJob(CopyJob.class, copyConfig);
            launchUniverseDataUnit = sparkJobResult.getTargets().get(0);
        } else {
            version = parseDataCollectionVersion(configuration);
            attrRepo = parseAttrRepo(configuration);
            evaluationDate = parseEvaluationDateStr(configuration);
            contactsDataExists = AttrRepoUtils.testExistsEntity(attrRepo, BusinessEntity.Contact);
            if (!contactsDataExists) {
                log.info("No Contact data found in the Attribute Repo");
            }
            // 1) setup queries from play and channel settings
            FrontEndQuery frontEndquery = new CampaignFrontEndQueryBuilder.Builder() //
                    .mainEntity(mainEntity) //
                    .customerSpace(customerSpace) //
                    .baseAccountRestriction(play.getTargetSegment().getAccountRestriction()) //
                    .baseContactRestriction(contactsDataExists ? play.getTargetSegment().getContactRestriction() : null)
                    .isSuppressAccountsWithoutLookupId(channelConfig.isSuppressAccountsWithoutLookupId()) //
                    .isSuppressAccountsWithoutContacts(
                            contactsDataExists && channelConfig.isSuppressAccountsWithoutContacts())
                    .isSuppressContactsWithoutEmails(contactsDataExists && channelConfig.isSuppressContactsWithoutEmails())
                    .isSuppressAccountsWithoutWebsiteOrCompanyName(ChannelConfigUtil.shouldApplyAccountNameOrWebsiteFilter(
                            channel.getLookupIdMap().getExternalSystemName(), channelConfig))
                    .bucketsToLaunch(launchBuckets) //
                    .limit(maxEntitiesToLaunch, useContactsPerAccountLimit) //
                    .lookupId(lookupId) //
                    .launchUnScored(launchUnScored) //
                    .destinationSystemName(externalSystemName) //
                    .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null) //
                    .getCampaignFrontEndQueryBuilder() //
                    .build();
            log.info("Full Launch Universe Query: " + frontEndquery.toString());
            // 2) get DataFrame for Account and Contact
            launchUniverseDataUnit = executeSparkJob(frontEndquery, maxEntitiesToLaunch);
            log.info(getHDFSDataUnitLogEntry("CurrentLaunchUniverse after first sparkjob", launchUniverseDataUnit));
            // 3) check for 'Contacts per Account' limit
            if (useSparkJobContactsPerAccount) {
                launchUniverseDataUnit = executeSparkJobContactsPerAccount(launchUniverseDataUnit,
                        maxContactsPerAccount, maxEntitiesToLaunch, customerSpace, contactAccountRatioThreshold);
                log.info(getHDFSDataUnitLogEntry("CurrentLaunchUniverse after second sparkjob", launchUniverseDataUnit));
            }
        }
        putObjectInContext(FULL_LAUNCH_UNIVERSE, launchUniverseDataUnit);
    }

    private FrontEndQuery buildFrontEndQuery(FrontEndQuery frontEndQuery, BusinessEntity entity) {
        FrontEndQuery result = new FrontEndQuery();
        result.setMainEntity(entity);
        result.setContactRestriction(frontEndQuery.getContactRestriction());
        result.setAccountRestriction(frontEndQuery.getAccountRestriction());
        return result;
    }

    private HdfsDataUnit executeSparkJob(FrontEndQuery frontEndQuery, Long maxEntitiesToLaunch) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                long userConfiguredLimit = maxEntitiesToLaunch == null ? 0 : maxEntitiesToLaunch;
                if (frontEndQuery.getMainEntity() == BusinessEntity.Account) {
                    long accountsCount = getEntityQueryCount(buildFrontEndQuery(frontEndQuery, BusinessEntity.Account));
                    campaignLaunchUtils.checkCampaignLaunchAccountLimitation(limitToCheck(userConfiguredLimit, accountsCount));
                } else {
                    if (contactsDataExists) {
                        long contactsCount = getEntityQueryCount(buildFrontEndQuery(frontEndQuery, BusinessEntity.Contact));
                        campaignLaunchUtils.checkCampaignLaunchContactLimitation(limitToCheck(userConfiguredLimit, contactsCount));
                    }
                }
                HdfsDataUnit launchDataUniverseDataUnit = getEntityQueryData(frontEndQuery, true);
                log.info("FullLaunchUniverseDataUnit: " + JsonUtils.serialize(launchDataUniverseDataUnit));
                return launchDataUniverseDataUnit;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private HdfsDataUnit executeSparkJobContactsPerAccount(HdfsDataUnit launchDataUniverseDataUnit, //
            Long maxContactsPerAccount, Long maxEntitiesToLaunch, CustomerSpace customerSpace,
            Long contactAccountRatioThreshold) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                List<String> sortConfig = WorkflowJobUtils.getSortConfigFromZK(customerSpace);
                String sortAttr = sortConfig.get(0);
                String sortDir = sortConfig.get(1);
                Lookup lookup = new AttributeLookup(BusinessEntity.Contact, sortAttr);
                HdfsDataUnit contactDataUnit = null;
                if (lookup != null) {
                    Set<Lookup> lookupSet = new HashSet<>();
                    Lookup contactId = new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name());
                    lookupSet.addAll(Arrays.asList(lookup, contactId));
                    FrontEndQuery query = new FrontEndQuery();
                    query.setLookups(new ArrayList<>(lookupSet));
                    query.setMainEntity(BusinessEntity.Contact);
                    contactDataUnit = getEntityQueryData(query);
                }
                List<DataUnit> inputUnits = new ArrayList<>();
                inputUnits.add(launchDataUniverseDataUnit);
                GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig(
                        getRandomWorkspace(), maxContactsPerAccount, maxEntitiesToLaunch, sortAttr, sortDir,
                        contactDataUnit, contactAccountRatioThreshold);
                config.setInput(inputUnits);
                log.info("Executing GenerateLaunchUniverseJob with config: " + JsonUtils.serialize(config));
                SparkJobResult result = executeSparkJob(GenerateLaunchUniverseJob.class, config);
                log.info("GenerateLaunchUniverseJob Results: " + JsonUtils.serialize(result));
                HdfsDataUnit launchUniverseDataUnit = result.getTargets().get(0);
                return launchUniverseDataUnit;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private long limitToCheck(long userConfiguredLimit, long queryCount) {
        return userConfiguredLimit > 0 ? Math.min(userConfiguredLimit, queryCount) : queryCount;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateLaunchUniverseStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(
            GenerateLaunchUniverseStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(GenerateLaunchUniverseStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(GenerateLaunchUniverseStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }

    private String getHDFSDataUnitLogEntry(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        return tag + ", " + JsonUtils.serialize(dataUnit);
    }

    protected boolean useSparkJobContactsPerAccount(BusinessEntity mainEntity, Long maxContactsPerAccount, Long contactAccountRatioThreshold) {
        return (mainEntity == BusinessEntity.Contact) && (maxContactsPerAccount != null || contactAccountRatioThreshold != null);
    }

    protected boolean useContactPerAccountLimit(BusinessEntity mainEntity, Long maxContactsPerAccount) {
        return (mainEntity == BusinessEntity.Contact) && (maxContactsPerAccount != null);
    }

}
