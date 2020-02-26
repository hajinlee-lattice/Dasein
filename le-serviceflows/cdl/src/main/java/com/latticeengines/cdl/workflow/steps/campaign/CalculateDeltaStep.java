package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.CalculateDeltaJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("calculateDelta")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculateDeltaStep extends BaseSparkSQLStep<CalculateDeltaStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(CalculateDeltaStep.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;

    @Override
    public void execute() {
        CalculateDeltaStepConfiguration config = getConfiguration();
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
            previousLaunchUniverse = (channel.getLaunchType() == LaunchType.DELTA
                    && !channel.getResetDeltaCalculationData() && previousAccountUniverseTable != null)
                            ? HdfsDataUnit.fromPath(previousAccountUniverseTable.getExtracts().get(0).getPath())
                            : null;
            log.info(getHDFSDataUnitLogEntry("PreviousAccountLaunchUniverse_", previousLaunchUniverse));
        } else {
            Table previousContactUniverseTable = StringUtils
                    .isNotBlank(channel.getCurrentLaunchedContactUniverseTable())
                            ? metadataProxy.getTable(configuration.getCustomerSpace().getTenantId(),
                                    channel.getCurrentLaunchedContactUniverseTable())
                            : null;

            previousLaunchUniverse = (channel.getLaunchType() == LaunchType.DELTA
                    && !channel.getResetDeltaCalculationData() && previousContactUniverseTable != null)
                            ? HdfsDataUnit.fromPath(previousContactUniverseTable.getExtracts().get(0).getPath())
                            : null;

            log.info(getHDFSDataUnitLogEntry("PreviousContactLaunchUniverse: ", previousLaunchUniverse));
        }

        HdfsDataUnit currentLaunchUniverse = getObjectFromContext(FULL_LAUNCH_UNIVERSE, HdfsDataUnit.class);
        log.info(getHDFSDataUnitLogEntry("CurrentLaunchUniverse: ", currentLaunchUniverse));

        // 2) compare previous launch universe to current launch universe

        SparkJobResult deltaCalculationResult = executeSparkJob(currentLaunchUniverse, previousLaunchUniverse,
                channel.getChannelConfig().getAudienceType().getInterfaceName(),
                channel.getChannelConfig().isSuppressAccountsWithoutContacts()
                        && channel.getChannelConfig().getAudienceType() == AudienceType.CONTACTS
                                ? AudienceType.ACCOUNTS.getInterfaceName()
                                : null,
                channel.getChannelConfig().isSuppressAccountsWithoutContacts());

        // 3) Generate Metadata tables for delta results
        processDeltaCalculationResult(channel.getChannelConfig().getAudienceType(), deltaCalculationResult);
    }

    private SparkJobResult executeSparkJob(HdfsDataUnit currentLaunchUniverse, HdfsDataUnit previousLaunchUniverse,
            String primaryJoinKey, String secondaryJoinKey, boolean filterJoinKeyNulls) {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig(currentLaunchUniverse, previousLaunchUniverse,
                primaryJoinKey, secondaryJoinKey, filterJoinKeyNulls, getRandomWorkspace());
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                log.info("Executing CalculateDeltaJob with config: " + JsonUtils.serialize(config));
                SparkJobResult result = executeSparkJob(CalculateDeltaJob.class, config);
                result.getTargets().add(currentLaunchUniverse);
                log.info("CalculateDeltaJob Results: " + JsonUtils.serialize(result));
                return result;
            } finally {
                stopSparkSQLSession();
            }
        });

    }

    private void processDeltaCalculationResult(AudienceType audienceType, SparkJobResult deltaCalculationResult) {
        CalculateDeltaStepConfiguration config = getConfiguration();

        HdfsDataUnit positiveDelta = deltaCalculationResult.getTargets().get(0);
        if (positiveDelta != null && positiveDelta.getCount() > 0) {
            processHDFSDataUnit(
                    String.format("Added%ss_%s", audienceType.asBusinessEntity().name(), config.getExecutionId()),
                    positiveDelta, audienceType.getInterfaceName(),
                    getAddDeltaTableContextKeyByAudienceType(audienceType), false);
        } else {
            log.info(String.format("No new Added %ss", audienceType.asBusinessEntity().name()));
        }

        HdfsDataUnit negativeDelta = deltaCalculationResult.getTargets().get(1);
        if (negativeDelta != null && negativeDelta.getCount() > 0) {
            processHDFSDataUnit(
                    String.format("Removed%ss_%s", audienceType.asBusinessEntity().name(), config.getExecutionId()),
                    negativeDelta, audienceType.getInterfaceName(),
                    getRemoveDeltaTableContextKeyByAudienceType(audienceType), false);
        } else {
            log.info(String.format("No %ss to be removed", audienceType.asBusinessEntity().name()));
        }

        HdfsDataUnit fullUniverse = deltaCalculationResult.getTargets().get(2);
        processHDFSDataUnit(
                String.format("Full%sUniverse_%s", audienceType.asBusinessEntity().name(), config.getExecutionId()),
                fullUniverse, audienceType.getInterfaceName(), getFullUniverseContextKeyByAudienceType(audienceType),
                true);

        log.info("Counts: "
                + JsonUtils.serialize(getMapObjectFromContext(DELTA_TABLE_COUNTS, String.class, Long.class)));
    }

    private void processHDFSDataUnit(String tableName, HdfsDataUnit dataUnit, String primaryKey, String contextKey,
            boolean createTable) {
        log.info(getHDFSDataUnitLogEntry(tableName, dataUnit));
        if (createTable) {
            Table dataUnitTable = toTable(tableName, primaryKey, dataUnit);
            metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
            putObjectInContext(contextKey, tableName);
            log.info("Created " + tableName + " at " + dataUnitTable.getExtracts().get(0).getPath());
        }
        putObjectInContext(contextKey + ATLAS_EXPORT_DATA_UNIT, dataUnit);
        Map<String, Long> counts = getMapObjectFromContext(DELTA_TABLE_COUNTS, String.class, Long.class);
        if (MapUtils.isEmpty(counts)) {
            counts = new HashMap<>();
        }
        counts.put(contextKey, dataUnit.getCount());
        putObjectInContext(DELTA_TABLE_COUNTS, counts);

    }

    private String getHDFSDataUnitLogEntry(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        return tag + ", " + JsonUtils.serialize(dataUnit);
    }

    private String getAddDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return ADDED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return ADDED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }

    private String getRemoveDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return REMOVED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return REMOVED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }

    private String getFullUniverseContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return FULL_ACCOUNTS_UNIVERSE;
        case CONTACTS:
            return FULL_CONTACTS_UNIVERSE;
        default:
            return null;
        }
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
