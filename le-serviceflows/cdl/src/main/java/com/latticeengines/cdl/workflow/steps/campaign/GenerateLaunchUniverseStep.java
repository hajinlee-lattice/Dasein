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
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLaunchUniverseStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("generateLaunchableUniverseStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateLaunchUniverseStep extends BaseSparkSQLStep<GenerateLaunchUniverseStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchUniverseStep.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;

    @Override
    public void execute() {
        GenerateLaunchUniverseStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();

        Play play = playProxy.getPlay(customerSpace.getTenantId(), config.getPlayId());
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

        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
        ChannelConfig channelConfig = channel.getChannelConfig();
        Set<RatingBucketName> launchBuckets = channel.getBucketsToLaunch();
        String lookupId = channel.getLookupIdMap().getAccountId();
        boolean launchUnscored = channel.isLaunchUnscored();
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();

        // 1) setup queries from play and channel settings
        FrontEndQuery frontEndquery = new CampaignFrontEndQueryBuilder.Builder() //
                .mainEntity(channelConfig.getAudienceType().asBusinessEntity()) //
                .customerSpace(customerSpace) //
                .baseAccountRestriction(play.getTargetSegment().getAccountRestriction()) //
                .baseContactRestriction(play.getTargetSegment().getContactRestriction())
                .isSuppressAccountsWithoutLookupId(
                        channelConfig instanceof SalesforceChannelConfig && ((SalesforceChannelConfig) channelConfig)
                                .isSuppressAccountsWithoutLookupId() == Boolean.TRUE) //
                .isSuppressAccountsWithoutContacts(channelConfig.isSuppressAccountsWithoutContacts())
                .isSuppressContactsWithoutEmails(channelConfig.isSuppressContactsWithoutEmails())
                .bucketsToLaunch(launchBuckets) //
                .lookupId(lookupId) //
                .launchUnscored(launchUnscored) //
                .destinationSystemName(externalSystemName) //
                .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null) //
                .getCampaignFrontEndQueryBuilder().build();

        log.info("Full Launch Universe Query: " + frontEndquery.toString());

        HdfsDataUnit launchUniverseDataUnit = executeSparkJob(frontEndquery);
        putObjectInContext(FULL_LAUNCH_UNIVERSE, launchUniverseDataUnit);
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

                // 2. get DataFrame for Account and Contact
                HdfsDataUnit launchDataUniverseDataUnit = getEntityQueryData(frontEndQuery);
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
}
