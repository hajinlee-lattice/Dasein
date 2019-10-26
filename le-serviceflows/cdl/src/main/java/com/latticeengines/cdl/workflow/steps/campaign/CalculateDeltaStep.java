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
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
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

        HdfsDataUnit previousLaunchUniverse;

        if (channel.getChannelConfig().getAudienceType() == AudienceType.ACCOUNTS) {
            Table previousAccountUniverseTable = StringUtils
                    .isNotBlank(channel.getCurrentLaunchedAccountUniverseTable())
                            ? metadataProxy.getTable(configuration.getCustomerSpace().getTenantId(),
                                    channel.getCurrentLaunchedAccountUniverseTable())
                            : null;
            previousLaunchUniverse = (channel.getLaunchType() == LaunchType.DIFFERENTIAL
                    && !channel.getResetDeltaCalculationData() && previousAccountUniverseTable != null)
                            ? HdfsDataUnit.fromPath(previousAccountUniverseTable.getExtracts().get(0).getPath())
                            : null;
            log.info(logHDFSDataUnit("PreviousAccountLaunchUniverse_", previousLaunchUniverse));
        } else {
            Table previousContactUniverseTable = StringUtils
                    .isNotBlank(channel.getCurrentLaunchedContactUniverseTable())
                            ? metadataProxy.getTable(configuration.getCustomerSpace().getTenantId(),
                                    channel.getCurrentLaunchedContactUniverseTable())
                            : null;

            previousLaunchUniverse = (channel.getLaunchType() == LaunchType.DIFFERENTIAL
                    && !channel.getResetDeltaCalculationData() && previousContactUniverseTable != null)
                            ? HdfsDataUnit.fromPath(previousContactUniverseTable.getExtracts().get(0).getPath())
                            : null;

            log.info(logHDFSDataUnit("PreviousContactLaunchUniverse_", previousLaunchUniverse));
        }

        HdfsDataUnit currentLaunchUniverse = getObjectFromContext(FULL_LAUNCH_UNIVERSE, HdfsDataUnit.class);

        // 2) compare previous launch universe to current launch universe

        SparkJobResult deltaCalculationResult = executeSparkJob(currentLaunchUniverse, previousLaunchUniverse,
                channel.getChannelConfig().getAudienceType() == AudienceType.ACCOUNTS ? InterfaceName.AccountId.name()
                        : InterfaceName.ContactId.name(),
                channel.getChannelConfig().isSuppressAccountsWithoutContacts());

        // 3) Generate Metadata tables for delta results
        processDeltaCalculationResult(channel.getChannelConfig().getAudienceType(), deltaCalculationResult, config);
    }

    private SparkJobResult executeSparkJob(HdfsDataUnit previousLaunchUniverse, HdfsDataUnit currentLaunchUniverse,
            String joinKey, boolean filterJoinKeyNulls) {

        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);

                SparkJobResult result = executeSparkJob(CalculateDeltaJob.class, new CalculateDeltaJobConfig(
                        currentLaunchUniverse, previousLaunchUniverse, joinKey, filterJoinKeyNulls));
                result.getTargets().add(currentLaunchUniverse);
                return result;
            } finally {
                stopSparkSQLSession();
            }
        });

    }

    private void processDeltaCalculationResult(AudienceType audienceType, SparkJobResult deltaCalculationResult,
            CalculateDeltaStepConfiguration config) {

        if (audienceType == AudienceType.ACCOUNTS) {
            HdfsDataUnit addedAccounts = deltaCalculationResult.getTargets().get(0);
            if (addedAccounts != null && addedAccounts.getCount() > 0) {
                processHDFSDataUnit("AddedAccounts_" + config.getExecutionId(), addedAccounts,
                        InterfaceName.AccountId.name(), ADDED_ACCOUNTS_DELTA_TABLE);
            } else {
                log.info("No new Added accounts");
            }

            HdfsDataUnit removedAccounts = deltaCalculationResult.getTargets().get(1);
            if (removedAccounts != null && removedAccounts.getCount() > 0) {
                processHDFSDataUnit("RemovedAccounts_" + config.getExecutionId(), removedAccounts,
                        InterfaceName.AccountId.name(), REMOVED_ACCOUNTS_DELTA_TABLE);
            } else {
                log.info("No removed accounts");
            }

            HdfsDataUnit fullAccountUniverse = deltaCalculationResult.getTargets().get(2);
            processHDFSDataUnit("FullAccountUniverse_" + config.getExecutionId(), fullAccountUniverse,
                    InterfaceName.AccountId.name(), FULL_ACCOUNTS_UNIVERSE);
        }

        if (audienceType == AudienceType.CONTACTS) {
            HdfsDataUnit addedContacts = deltaCalculationResult.getTargets().get(0);
            if (addedContacts != null && addedContacts.getCount() > 0) {
                processHDFSDataUnit("AddedContacts_" + config.getExecutionId(), addedContacts,
                        InterfaceName.ContactId.name(), ADDED_CONTACTS_DELTA_TABLE);
            } else {
                log.info("No new contacts to be added");
            }

            HdfsDataUnit removedContacts = deltaCalculationResult.getTargets().get(1);
            if (removedContacts != null && removedContacts.getCount() > 0) {
                processHDFSDataUnit("RemovedContacts_" + config.getExecutionId(), removedContacts,
                        InterfaceName.ContactId.name(), REMOVED_CONTACTS_DELTA_TABLE);
            } else {
                log.info("No removed contacts");
            }

            HdfsDataUnit fullContactUniverse = deltaCalculationResult.getTargets().get(2);
            processHDFSDataUnit("FullContactUniverse_" + config.getExecutionId(), fullContactUniverse,
                    InterfaceName.ContactId.name(), FULL_CONTACTS_UNIVERSE);
        }

    }

    private void processHDFSDataUnit(String tableName, HdfsDataUnit dataUnit, String primaryKey, String contextKey) {
        log.info(logHDFSDataUnit(tableName, dataUnit));
        Table dataUnitTable = toTable(tableName, primaryKey, dataUnit);
        metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
        putObjectInContext(contextKey, tableName);
        log.info("Created " + tableName + " at " + dataUnitTable.getExtracts().get(0).getPath());
    }

    private String logHDFSDataUnit(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        String valueSeparator = ": ";
        String tokenSeparator = ", ";
        return tag + tokenSeparator //
                + "StorageType: " + valueSeparator + dataUnit.getStorageType().name() + tokenSeparator //
                + "Path: " + valueSeparator + dataUnit.getPath() + tokenSeparator //
                + "Count: " + valueSeparator + dataUnit.getCount();
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
