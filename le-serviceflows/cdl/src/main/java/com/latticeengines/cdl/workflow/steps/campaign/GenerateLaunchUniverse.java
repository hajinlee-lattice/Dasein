package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

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
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLaunchUniverseStepConfiguration;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

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
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No Campaign found by ID: " + config.getPlayId() });
        }

        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No Channel found by ID: " + config.getChannelId() });
        }

        PlayLaunch launch = null;
        if (StringUtils.isNotBlank(config.getLaunchId())) {
            launch = playProxy.getPlayLaunch(customerSpace.getTenantId(), config.getPlayId(), config.getLaunchId());
        }

        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
        contactsDataExists = doesContactDataExist(attrRepo);

        ChannelConfig channelConfig = launch == null ? channel.getChannelConfig() : launch.getChannelConfig();
        Set<RatingBucketName> launchBuckets = launch == null ? channel.getBucketsToLaunch()
                : launch.getBucketsToLaunch();
        String lookupId = launch == null ? channel.getLookupIdMap().getAccountId() : launch.getDestinationAccountId();
        boolean launchUnScored = launch == null ? channel.getLaunchUnscored() : launch.isLaunchUnscored();
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();

        // 1) setup queries from play and channel settings
        FrontEndQuery frontEndquery = new CampaignFrontEndQueryBuilder.Builder() //
                .mainEntity(channelConfig.getAudienceType().asBusinessEntity()) //
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
                .limit(channel.getMaxAccountsToLaunch()) //
                .lookupId(lookupId) //
                .launchUnScored(launchUnScored) //
                .destinationSystemName(externalSystemName) //
                .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null) //
                .getCampaignFrontEndQueryBuilder() //
                .build();

        log.info("Full Launch Universe Query: " + frontEndquery.toString());

        HdfsDataUnit launchUniverseDataUnit = executeSparkJob(frontEndquery);
        log.info(getHDFSDataUnitLogEntry("CurrentLaunchUniverse", launchUniverseDataUnit));
        putObjectInContext(FULL_LAUNCH_UNIVERSE, launchUniverseDataUnit);
    }

    private boolean doesContactDataExist(AttributeRepository attrRepo) {
        try {
            AttrRepoUtils.getTablePath(attrRepo, BusinessEntity.Contact);
            return true;
        } catch (QueryEvaluationException e) {
            log.info("No Contact data found in the Attribute Repo");
            return false;
        }
    }

    private FrontEndQuery buildFrontEndQuery(FrontEndQuery frontEndQuery, BusinessEntity entity) {
        FrontEndQuery result = new FrontEndQuery();
        result.setMainEntity(entity);
        result.setContactRestriction(frontEndQuery.getContactRestriction());
        result.setAccountRestriction(frontEndQuery.getAccountRestriction());
        return result;
    }

    private HdfsDataUnit executeSparkJob(FrontEndQuery frontEndQuery) {

        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                long accountsCount = getEntityQueryCount(buildFrontEndQuery(frontEndQuery, BusinessEntity.Account));
                campaignLaunchUtils.checkCampaignLaunchAccountLimitation(accountsCount);
                if (contactsDataExists) {
                    long contactsCount = getEntityQueryCount(buildFrontEndQuery(frontEndQuery, BusinessEntity.Contact));
                    campaignLaunchUtils.checkCampaignLaunchContactLimitation(contactsCount);
                }
                // 2. get DataFrame for Account and Contact
                HdfsDataUnit launchDataUniverseDataUnit = getEntityQueryData(frontEndQuery, true);

                log.info("FullLaunchUniverseDataUnit: " + JsonUtils.serialize(launchDataUniverseDataUnit));
                return launchDataUniverseDataUnit;
            } finally {
                stopSparkSQLSession();
            }
        });

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
}
