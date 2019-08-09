package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

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
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CalculateDeltaStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.CalculateDeltaJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("calculateDeltaStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculateDeltaStep extends BaseSparkSQLStep<CalculateDeltaStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(CalculateDeltaStep.class);

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
        CalculateDeltaStepConfiguration config = getConfiguration();
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

        // 1) setup queries from play and channel settings
        FrontEndQuery accountQuery = new CampaignFrontEndQueryBuilder.Builder().mainEntity(BusinessEntity.Account)
                .customerSpace(customerSpace).targetSegmentRestriction(play.getTargetSegment().getAccountRestriction())
                .isSupressAccountWithoutLookupId(channel.getChannelConfig() instanceof SalesforceChannelConfig
                        && ((SalesforceChannelConfig) channel.getChannelConfig())
                                .isSupressAccountsWithoutLookupId() == Boolean.TRUE)
                .bucketsToLaunch(channel.getBucketsToLaunch()).lookupId(channel.getLookupIdMap().getAccountId())
                .launchUnscored(channel.isLaunchUnscored())
                .destinationSystemName(channel.getLookupIdMap().getExternalSystemName())
                .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null)
                .getCampaignFrontEndQueryBuilder().build();

        log.info("To Launch Account Universe Query: " + accountQuery.toString());

        FrontEndQuery contactQuery = new CampaignFrontEndQueryBuilder.Builder().mainEntity(BusinessEntity.Contact)
                .customerSpace(customerSpace).targetSegmentRestriction(play.getTargetSegment().getContactRestriction())
                .isSupressAccountWithoutLookupId(channel.getChannelConfig() instanceof SalesforceChannelConfig
                        && ((SalesforceChannelConfig) channel.getChannelConfig())
                                .isSupressAccountsWithoutLookupId() == Boolean.TRUE)
                .bucketsToLaunch(channel.getBucketsToLaunch()).lookupId(channel.getLookupIdMap().getAccountId())
                .launchUnscored(channel.isLaunchUnscored())
                .destinationSystemName(channel.getLookupIdMap().getExternalSystemName())
                .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null)
                .getCampaignFrontEndQueryBuilder().build();

        log.info("To Launch Contact Universe Query: " + contactQuery.toString());

        // 2) compare previous launch universe to current launch universe

        SparkJobResult deltaCalculationResult = executeSparkJob(accountQuery, contactQuery, channel.getLaunchType());

        // 3) save current launch universe and delta as files
        HdfsDataUnit addedAccounts = deltaCalculationResult.getTargets().get(0);
        log.info(logHDFSDataUnit("Added Accounts", addedAccounts));

        HdfsDataUnit removedAccounts = deltaCalculationResult.getTargets().get(1);
        log.info(logHDFSDataUnit("Removed Accounts", removedAccounts));

        HdfsDataUnit addedContacts = deltaCalculationResult.getTargets().get(2);
        log.info(logHDFSDataUnit("Added Contacts", addedContacts));

        HdfsDataUnit removedContacts = deltaCalculationResult.getTargets().get(3);
        log.info(logHDFSDataUnit("Removed Contacts", removedContacts));

        HdfsDataUnit newAccountUniverse = deltaCalculationResult.getTargets().get(4);
        log.info(logHDFSDataUnit("Full Accounts", newAccountUniverse));

        HdfsDataUnit newContactUniverse = deltaCalculationResult.getTargets().get(5);
        log.info(logHDFSDataUnit("Full Contacts", newContactUniverse));

    }

    private SparkJobResult executeSparkJob(FrontEndQuery accountQuery, FrontEndQuery contactQuery, LaunchType type) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(1); // todo: make it 3 later

        HdfsDataUnit previousAccountUniverse = (type == LaunchType.DIFFERENTIAL)
                ? WorkflowStaticContext.getObject(PlayLaunchChannel.PREVIOUS_ACCOUNT_UNIVERSE, HdfsDataUnit.class)
                : null;

        HdfsDataUnit previousContactUniverse = (type == LaunchType.DIFFERENTIAL)
                ? WorkflowStaticContext.getObject(PlayLaunchChannel.PREVIOUS_CONTACT_UNIVERSE, HdfsDataUnit.class)
                : null;

        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);

                // 2. get DataFrame for Account and Contact
                HdfsDataUnit accountDataUnit = getEntityQueryData(accountQuery);
                log.info("accountDataUnit: " + JsonUtils.serialize(accountDataUnit));
                HdfsDataUnit contactDataUnit = null;
                String contactTableName = attrRepo.getTableName(TableRoleInCollection.SortedContact);
                if (StringUtils.isBlank(contactTableName)) {
                    log.info("No contact table available in Redshift.");
                } else {
                    contactDataUnit = getEntityQueryData(contactQuery);
                    log.info("contactDataUnit: " + JsonUtils.serialize(contactDataUnit));
                }

                // 3. generate avro out of DataFrame with predefined format for Recommendations
                return executeSparkJob(CalculateDeltaJob.class, buildCalculateDeltaJobConfig(accountDataUnit,
                        contactDataUnit, previousAccountUniverse, previousContactUniverse));
            } finally {
                stopSparkSQLSession();
            }
        });

    }

    private String logHDFSDataUnit(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }

        String valueSeparator = ": ";
        String tokenSparator = ", ";
        return tag + tokenSparator //
                + "StorageType: " + valueSeparator + dataUnit.getStorageType().name() + tokenSparator //
                + "Path: " + valueSeparator + dataUnit.getPath();
    }

    private CalculateDeltaJobConfig buildCalculateDeltaJobConfig(HdfsDataUnit accountDataUnit,
            HdfsDataUnit contactDataUnit, HdfsDataUnit previousAccountUniverse, HdfsDataUnit previousContactUniverse) {
        CalculateDeltaJobConfig calculateDeltaConfig = new CalculateDeltaJobConfig();
        calculateDeltaConfig.setWorkspace(getRandomWorkspace());
        calculateDeltaConfig.setCurrentAccountUniverse(accountDataUnit);
        calculateDeltaConfig.setCurrentContactUniverse(contactDataUnit);
        calculateDeltaConfig.setPreviousAccountUniverse(previousAccountUniverse);
        calculateDeltaConfig.setPreviousContactUniverse(previousContactUniverse);
        return calculateDeltaConfig;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(CalculateDeltaStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(CalculateDeltaStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(CalculateDeltaStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(CalculateDeltaStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }
}
