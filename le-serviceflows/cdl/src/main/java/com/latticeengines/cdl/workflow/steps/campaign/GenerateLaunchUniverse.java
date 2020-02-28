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

import com.latticeengines.cdl.workflow.steps.campaign.utils.CampaignLaunchUtils;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
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
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
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
    private CampaignLaunchUtils campaignLaunchUtils;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;
    private ChannelConfig channelConfig;
    private PlayLaunchChannel channel;
    private boolean contactsDataExists;

    @Override
    public void execute() {
        GenerateLaunchUniverseStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        Play play = campaignLaunchUtils.getPlay(customerSpace, config.getPlayId(), false, false);
        channel = campaignLaunchUtils.getPlayLaunchChannel(customerSpace, config.getPlayId(), config.getChannelId());
        PlayLaunch launch = campaignLaunchUtils.getPlayLaunch(customerSpace, config.getPlayId(), config.getLaunchId());
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
        contactsDataExists = doesContactDataExist(attrRepo);
        channelConfig = launch == null ? channel.getChannelConfig() : launch.getChannelConfig();
        Set<RatingBucketName> launchBuckets = launch == null ? channel.getBucketsToLaunch()
                : launch.getBucketsToLaunch();
        String lookupId = launch == null ? channel.getLookupIdMap().getAccountId() : launch.getDestinationAccountId();
        boolean launchUnScored = launch == null ? channel.getLaunchUnscored() : launch.isLaunchUnscored();
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();
        // 1) setup queries from play and channel settings
        FrontEndQuery frontEndquery = campaignLaunchUtils.buildCampaignFrontEndQuery(customerSpace, channelConfig,
                play, contactsDataExists, launchUnScored, launchBuckets, externalSystemName, channel.getMaxAccountsToLaunch(), lookupId);
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

    private HdfsDataUnit executeSparkJob(FrontEndQuery frontEndQuery) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                // 2. check the account and account limit
                campaignLaunchUtils.checkCampaignLaunchLimitation(frontEndQuery, contactsDataExists,
                        channel.getMaxAccountsToLaunch(), (query, entity) -> getEntityCount(query, entity));
                // 3. get DataFrame for Account and Contact
                HdfsDataUnit launchDataUniverseDataUnit = getEntityQueryData(frontEndQuery.getDeepCopy(), true);
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
