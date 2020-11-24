package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ActivityAlert;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityAlertJobConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateActivityAlertJob;

@Component(GenerateActivityAlerts.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateActivityAlerts extends RunSparkJob<TimeLineSparkStepConfiguration, ActivityAlertJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(GenerateActivityAlerts.class);

    static final String BEAN_NAME = "generateActivityAlerts";

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private Long currentTimestamp;

    @Override
    protected ActivityAlertJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        if (isShortCutMode()) {
            linkAlertTablesToInactive();
            return null;
        }
        List<ActivityAlertsConfig> alertConfigs = getAlertConfigs();
        if (!shouldExecute(alertConfigs)) {
            return null;
        }

        Table timeLineMasterTable = getAccount360TimeLineMasterTable();
        Table activityAlertMasterTable = getActivityAlertMasterTable();
        String activityAlertVersion = getOrCreateActivityAlertVersion();
        Long lastEvaluationTime = getLastAlertEvaluationTime();
        currentTimestamp = getCurrentTimestamp();
        boolean dedupAlert = lastEvaluationTime == null || currentTimestamp.equals(lastEvaluationTime);
        Preconditions.checkNotNull(timeLineMasterTable, "should have account timeline master table");

        log.info("Last evaluation time = {}, current time = {}, shouldDedupAlert = {}", lastEvaluationTime,
                currentTimestamp, dedupAlert);

        List<DataUnit> inputs = new ArrayList<>();
        inputs.add(timeLineMasterTable.toHdfsDataUnit("AccountTimeLine"));

        ActivityAlertJobConfig config = new ActivityAlertJobConfig();
        config.currentEpochMilli = currentTimestamp;
        config.masterAccountTimeLineIdx = 0;
        config.dedupAlert = dedupAlert;
        config.alertNameToQualificationPeriodDays.putAll(alertConfigs.stream() //
                .filter(Objects::nonNull) //
                .filter(ActivityAlertsConfig::isActive) //
                .map(alertConfig -> Pair.of(alertConfig.getName(), alertConfig.getQualificationPeriodDays())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)));
        if (activityAlertMasterTable != null) {
            config.masterAlertIdx = 1;
            inputs.add(activityAlertMasterTable.toHdfsDataUnit("ActivityAlert"));
        }
        config.setInput(inputs);

        log.info(
                "Generating activity alerts. timeline table = {},"
                        + " alert master table = {}, alert configs = {}, spark config = {}, alert version = {}",
                timeLineMasterTable.getName(),
                activityAlertMasterTable == null ? null : activityAlertMasterTable.getName(),
                JsonUtils.serialize(alertConfigs), JsonUtils.serialize(config), activityAlertVersion);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        boolean hasNewAlerts = Long.parseLong(result.getOutput()) > 0L;
        putObjectInContext(ACTIVITY_ALERT_GENERATED, true);
        if (!hasNewAlerts) {
            log.info("No new activity alert generated, no need to create/link alert table. count = {}",
                    result.getOutput());
        } else {
            // [ master table, diff table ]
            List<HdfsDataUnit> outputs = result.getTargets();
            Preconditions.checkArgument(CollectionUtils.size(outputs) == 2, String.format(
                    "Activity alert spark job should output two tables " + "(master alert, diff alert), got %d instead",
                    CollectionUtils.size(outputs)));

            // create master table and link to inactive version
            String alertMasterTableName = customerSpace.getTenantId() + "_"
                    + NamingUtils.timestamp(ActivityAlert.name());
            createTableAndPersist(outputs.get(0), alertMasterTableName, ACTIVITY_ALERT_MASTER_TABLE_NAME);
            linkAlertTablesToInactive();

            // create diff table and set to context only
            String alertDiffTableName = customerSpace.getTenantId() + "_"
                    + NamingUtils.timestamp(ActivityAlert.name() + "Diff");
            createTableAndPersist(outputs.get(1), alertDiffTableName, ACTIVITY_ALERT_DIFF_TABLE_NAME);

            setLastAlertEvaluationTime();
        }
    }

    private void createTableAndPersist(@NotNull HdfsDataUnit unit, @NotNull String tableName, @NotNull String ctxKey) {
        Table table = toTable(tableName, unit);
        metadataProxy.createTable(customerSpace.toString(), tableName, table);
        exportToS3AndAddToContext(table, ctxKey);
    }

    private void linkAlertTablesToInactive() {
        String alertMasterTable = getStringValueFromContext(ACTIVITY_ALERT_MASTER_TABLE_NAME);
        if (StringUtils.isBlank(alertMasterTable)) {
            log.info("No alert master table generated, skip linking");
            return;
        }

        String tenant = configuration.getCustomer();
        log.info("Linking activity alert master table {} to inactive version {}", alertMasterTable, inactive);
        dataCollectionProxy.upsertTable(tenant, alertMasterTable, ActivityAlert, inactive);
    }

    private boolean shouldExecute(@NotNull List<ActivityAlertsConfig> alertConfigs) {
        if (alertConfigs.isEmpty()) {
            log.info("No activity alert configuration setup for tenant {}, skip step", configuration.getCustomer());
            return false;
        } else if (!hasAccountTimeLineChange() && !configuration.isShouldRebuild()) {
            log.info("No timeline diff table found in context (no new stream import)"
                    + " and not in rebuild mode, skip generating activity alerts");
            return false;
        }

        return true;
    }

    // TODO move timeline helpers to a shared base step

    private long getCurrentTimestamp() {
        String evaluationDateStr = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isNotBlank(evaluationDateStr)) {
            long currTime = LocalDate
                    .parse(evaluationDateStr, DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING)) //
                    .atStartOfDay(ZoneId.of("UTC")) // start of date in UTC
                    .toInstant() //
                    .toEpochMilli();
            log.info("Found evaluation date {}, use end of date as current time. Timestamp = {}", evaluationDateStr,
                    currTime);
            return currTime;
        } else {
            Long paTime = getLongValueFromContext(PA_TIMESTAMP);
            Preconditions.checkNotNull(paTime, "pa timestamp should be set in context");
            log.info("No evaluation date str found in context, use pa timestamp = {}", paTime);
            return paTime;
        }
    }

    private Table getAccount360TimeLineMasterTable() {
        Map<String, String> tableNames = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        String tableName = tableNames.get(account360TimelineId());
        return metadataProxy.getTableSummary(customerSpace.toString(), tableName);
    }

    private boolean hasAccountTimeLineChange() {
        Map<String, String> timelineTableNames = getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME, String.class,
                String.class);
        if (MapUtils.isEmpty(timelineTableNames)) {
            log.info("No account timeline change since there are no timeline diff tables found in ctx");
            return false;
        }

        log.info("Timeline diff table in context = {}", timelineTableNames);
        return timelineTableNames.containsKey(account360TimelineId());
    }

    private String account360TimelineId() {
        return TimeLineStoreUtils.contructTimelineId(configuration.getCustomer(),
                TimeLineStoreUtils.ACCOUNT360_TIMELINE_NAME);
    }

    private Table getActivityAlertMasterTable() {
        String tableName = getTableName(ActivityAlert, "activity alert master table");
        if (StringUtils.isBlank(tableName)) {
            return null;
        } else if (configuration.isShouldRebuild()) {
            log.info("In rebuild mode, ignore activity alert master table {}", tableName);
            return null;
        }
        return metadataProxy.getTableSummary(configuration.getCustomer(), tableName);
    }

    private String getTableName(@NotNull TableRoleInCollection role, @NotNull String name) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(tableName)) {
            tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(tableName)) {
                log.info("Found {} (role={}) in active version {}", name, role, active);
            }
        } else {
            log.info("Found {} (role={}) in inactive version {}", name, role, inactive);
        }
        return tableName;
    }

    private String getOrCreateActivityAlertVersion() {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        String version = dcStatus.getActivityAlertVersion();
        if (StringUtils.isBlank(version)) {
            version = UUID.randomUUID().toString();
            log.info("No existing activity alert version, creating a new one {}", version);
        } else if (configuration.isShouldRebuild()) {
            version = UUID.randomUUID().toString();
            log.info("In rebuild mode, creating a new activity alert version {}", version);
        }
        dcStatus.setActivityAlertVersion(version);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
        return version;
    }

    private Long getLastAlertEvaluationTime() {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        return MapUtils.emptyIfNull(dcStatus.getEvaluationDateMap()).get(ActivityAlert.name());
    }

    private void setLastAlertEvaluationTime() {
        Preconditions.checkNotNull(currentTimestamp, "Current time should be set already");
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus.getEvaluationDateMap() == null) {
            dcStatus.setEvaluationDateMap(new HashMap<>());
        }
        log.info("Set evaluation date for {} to {}", ActivityAlert.name(), currentTimestamp);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
    }

    private boolean isShortCutMode() {
        boolean alertGenerated = BooleanUtils.isTrue(getObjectFromContext(ACTIVITY_ALERT_GENERATED, Boolean.class));
        if (!alertGenerated) {
            return false;
        }

        String alertMasterTable = getStringValueFromContext(ACTIVITY_ALERT_MASTER_TABLE_NAME);
        String alertDiffTable = getStringValueFromContext(ACTIVITY_ALERT_DIFF_TABLE_NAME);
        if (StringUtils.isNotBlank(alertMasterTable)) {
            if (hasTableInCtxKey(configuration.getCustomer(), alertMasterTable)) {
                log.warn("In short-cut mode but missing generated alert master table {} in hdfs, re-execute this step",
                        alertMasterTable);
                return false;
            }
        }
        if (StringUtils.isNotBlank(alertDiffTable)) {
            if (hasTableInCtxKey(configuration.getCustomer(), alertDiffTable)) {
                log.warn("In short-cut mode but missing generated alert diff table {} in hdfs, re-execute this step",
                        alertDiffTable);
                return false;
            }
        }
        return true;
    }

    private List<ActivityAlertsConfig> getAlertConfigs() {
        List<ActivityAlertsConfig> configs = ListUtils
                .emptyIfNull(activityStoreProxy.getActivityAlertsConfiguration(configuration.getCustomer()));
        log.info("ActivityConfigs = {} for tenant {}", JsonUtils.serialize(configs), configuration.getCustomer());
        return configs;
    }

    @Override
    protected Class<? extends AbstractSparkJob<ActivityAlertJobConfig>> getJobClz() {
        return GenerateActivityAlertJob.class;
    }
}
