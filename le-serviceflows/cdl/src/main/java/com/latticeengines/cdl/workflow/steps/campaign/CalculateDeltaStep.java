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

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
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

        log.info("Full Account Universe Query: " + accountQuery.toString());

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

        log.info("Full Contact Universe Query: " + contactQuery.toString());

        // 2) compare previous launch universe to current launch universe

        SparkJobResult deltaCalculationResult = executeSparkJob(accountQuery, contactQuery, channel);

        // 3) save current launch universe and delta as files
        processDeltaCalculationResult(deltaCalculationResult, config);

        // 4) Record the new launch universe for the next delta calculation
        channel.setCurrentLaunchedAccountUniverseTable(getObjectFromContext(FULL_ACCOUNTS_UNIVERSE, Table.class));
        channel.setCurrentLaunchedContactUniverseTable(getObjectFromContext(FULL_CONTACTS_UNIVERSE, Table.class));
        playProxy.updatePlayLaunchChannel(customerSpace.getTenantId(), config.getPlayId(), config.getChannelId(),
                channel, false);
    }

    private void processDeltaCalculationResult(SparkJobResult deltaCalculationResult,
            CalculateDeltaStepConfiguration config) {
        HdfsDataUnit addedAccounts = deltaCalculationResult.getTargets().get(0);
        if (addedAccounts != null && addedAccounts.getCount() > 0) {
            processHDFSDataUnit(addedAccounts, InterfaceName.AccountId.name(), "AddedAccounts_",
                    ADDED_ACCOUNTS_DELTA_TABLE, config);
        } else {
            log.info("No new Added accounts");
        }

        HdfsDataUnit removedAccounts = deltaCalculationResult.getTargets().get(1);
        if (removedAccounts != null && removedAccounts.getCount() > 0) {
            processHDFSDataUnit(removedAccounts, InterfaceName.AccountId.name(), "RemovedAccounts_",
                    REMOVED_ACCOUNTS_DELTA_TABLE, config);
        } else {
            log.info("No removed accounts");
        }

        HdfsDataUnit addedContacts = deltaCalculationResult.getTargets().get(2);
        if (addedContacts != null && addedContacts.getCount() > 0) {
            processHDFSDataUnit(addedContacts, InterfaceName.ContactId.name(), "AddedContacts_",
                    ADDED_CONTACTS_DELTA_TABLE, config);
        } else {
            log.info("No new contacts to be added");
        }

        HdfsDataUnit removedContacts = deltaCalculationResult.getTargets().get(3);
        if (removedContacts != null && removedContacts.getCount() > 0) {
            processHDFSDataUnit(removedContacts, InterfaceName.ContactId.name(), "RemovedContacts_",
                    REMOVED_CONTACTS_DELTA_TABLE, config);
        } else {
            log.info("No removed contacts");
        }

        HdfsDataUnit fullAccountUniverse = deltaCalculationResult.getTargets().get(4);
        processHDFSDataUnit(fullAccountUniverse, InterfaceName.AccountId.name(), "FullAccountUniverse_",
                FULL_ACCOUNTS_UNIVERSE, config);

        HdfsDataUnit fullContactUniverse = deltaCalculationResult.getTargets().get(5);
        if (fullContactUniverse != null && fullContactUniverse.getCount() > 0) {
            processHDFSDataUnit(fullContactUniverse, InterfaceName.ContactId.name(), "FullContactUniverse_",
                    FULL_CONTACTS_UNIVERSE, config);
        } else {
            log.info("Contact universe is empty");
        }
    }

    private void processHDFSDataUnit(HdfsDataUnit dataUnit, String primaryKey, String tableNamePrefix,
            String contextKey, CalculateDeltaStepConfiguration config) {
        String tableName = NamingUtils.timestamp(tableNamePrefix);
        Table dataUnitTable = toTable(tableName, primaryKey, dataUnit,
                getTargetTablePath(config.getPlayId(), config.getChannelId(), config.getExecutionId(), tableName));
        metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
        dataUnitTable = metadataProxy.getTable(customerSpace.getTenantId(), dataUnitTable.getName());
        putObjectInContext(contextKey, dataUnitTable);
        log.info(logHDFSDataUnit(tableNamePrefix, dataUnit));
        log.info("Created " + tableName + " at " + dataUnitTable.getExtracts().get(0).getPath());
    }

    private String getTargetTablePath(String playId, String channelId, String executionId, String tableName) {
        return PathBuilder.buildDataTablePath(podId, customerSpace).append(PathConstants.CAMPAIGNS).append(playId)
                .append(channelId).append(executionId).append(tableName).toString();
    }

    private SparkJobResult executeSparkJob(FrontEndQuery accountQuery, FrontEndQuery contactQuery,
            PlayLaunchChannel channel) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(1); // todo: make it 3 later

        HdfsDataUnit previousAccountUniverse = (channel.getLaunchType() == LaunchType.DIFFERENTIAL
                && channel.getCurrentLaunchedAccountUniverseTable() != null)
                        ? HdfsDataUnit.fromPath(
                                channel.getCurrentLaunchedAccountUniverseTable().getExtracts().get(0).getPath())
                        : null;

        HdfsDataUnit previousContactUniverse = (channel.getLaunchType() == LaunchType.DIFFERENTIAL
                && channel.getCurrentLaunchedContactUniverseTable() != null)
                        ? HdfsDataUnit.fromPath(
                                channel.getCurrentLaunchedContactUniverseTable().getExtracts().get(0).getPath())
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
                return executeSparkJob(CalculateDeltaJob.class, //
                        buildCalculateDeltaJobConfig(accountDataUnit, contactDataUnit, //
                                previousAccountUniverse, previousContactUniverse));
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
