package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountJourneyStage;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateJourneyStageJob;

@Component(GenerateJourneyStage.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateJourneyStage extends RunSparkJob<TimeLineSparkStepConfiguration, JourneyStageJobConfig> {
    private static final Logger log = LoggerFactory.getLogger(GenerateJourneyStage.class);

    static final String BEAN_NAME = "generateJourneyStage";

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private TimeLine acc360Timeline;
    private List<JourneyStage> journeyStages;

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private String accountBatchStoreTableName;
    private String accountJourneyStageTableName;

    @Override
    protected JourneyStageJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        if (isShortCutMode()) {
            log.info("In short cut mode, skip generating journey stages");
            linkTimelineMasterTablesToInactive();
            linkJourneyStageTableToInactive();
            return null;
        }

        accountBatchStoreTableName = getAccountTableName();
        accountJourneyStageTableName = getAccountJourneyStageTableName();
        acc360Timeline = getAccount360TimeLine();
        journeyStages = activityStoreProxy.getJourneyStages(customerSpace.toString());

        if (!shouldExecute()) {
            return null;
        }

        Table masterTable = getAccount360TimeLineMasterTable();
        Table diffTable = getAccount360TimeLineDiffTable();
        if (masterTable == null || diffTable == null) {
            log.info(
                    "No master/diff table found for account timeline. skip generating journey stage."
                            + " master table = {}, diff table = {}",
                    masterTable == null ? null : masterTable.getName(), diffTable == null ? null : diffTable.getName());
            return null;
        }
        log.info(
                "Generating journey stages for accounts. account time line object = {}, master table name = {},"
                        + " diff table name = {}, hasAccountTimeLineChange = {}, rebuild = {}",
                JsonUtils.serialize(acc360Timeline), masterTable.getName(), diffTable.getName(),
                hasAccountTimeLineChange(), configuration.isShouldRebuild());

        List<DataUnit> inputs = new ArrayList<>();
        inputs.add(masterTable.toHdfsDataUnit("AccTimeLineMaster"));
        inputs.add(diffTable.toHdfsDataUnit("AccTimeLineDiff"));
        JourneyStageJobConfig config = new JourneyStageJobConfig();
        config.currentEpochMilli = getCurrentTimestamp();
        config.masterAccountTimeLineIdx = 0;
        config.diffAccountTimeLineIdx = 1;
        Pair<List<JourneyStage>, JourneyStage> processedStages = processJourneyStages(journeyStages);
        config.journeyStages = processedStages.getLeft();
        config.defaultStage = processedStages.getRight();
        config.accountTimeLineId = acc360Timeline.getTimelineId();
        config.accountTimeLineVersion = getTimeLineVersion(config.accountTimeLineId);
        if (StringUtils.isNotBlank(accountJourneyStageTableName)) {
            Table journeyStageTable = metadataProxy.getTableSummary(customerSpace.toString(),
                    accountJourneyStageTableName);
            log.info("Set current journey stage table {} to config", accountJourneyStageTableName);
            inputs.add(journeyStageTable.toHdfsDataUnit("AccJourneyStage"));
            config.masterJourneyStageIdx = 2;
        }

        log.info("Processed journey stage configuration = {}, default stage = {}, current time = {}",
                JsonUtils.serialize(config.journeyStages), JsonUtils.serialize(config.defaultStage),
                config.currentEpochMilli);

        config.setInput(inputs);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        // [ master table, diff table ]
        List<HdfsDataUnit> outputs = result.getTargets();
        Preconditions.checkArgument(CollectionUtils.size(outputs) == 3,
                String.format(
                        "Journey stage spark job should output three tables "
                                + "(master timeline, diff timeline, journey stage), got %d instead",
                        CollectionUtils.size(outputs)));
        // create updated timeline master/diff table and update corresponding contexts
        handleUpdatedAccount360TimelineTables(outputs);
        handleJourneyStageTable(outputs.get(2));

        // link all tables to inactive version
        linkTimelineMasterTablesToInactive();
    }

    // return [ list of non default stages, default journey stage ]
    private Pair<List<JourneyStage>, JourneyStage> processJourneyStages(List<JourneyStage> stages) {
        log.info("Journey stages for tenant {} = {}", customerSpace.toString(), JsonUtils.serialize(stages));
        Preconditions.checkArgument(CollectionUtils.size(stages) >= 1, "There should be at least one journey stage");

        // sorted by priority, the one with highest priority number is the default stage
        List<JourneyStage> sortedStages = stages.stream() //
                .sorted(Comparator.comparing(JourneyStage::getPriority)) //
                .collect(Collectors.toList());
        int size = sortedStages.size();
        return Pair.of(new ArrayList<>(sortedStages.subList(0, size - 1)), sortedStages.get(size - 1));
    }

    private String getTimeLineVersion(@NotNull String timelineId) {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus == null) {
            return null;
        }

        Map<String, String> versionMap = dcStatus.getTimelineVersionMap();
        String version = MapUtils.emptyIfNull(versionMap).get(timelineId);
        log.info("Timeline version map = {}, timeline id = {}, version = {}", versionMap, timelineId, version);
        return version;
    }

    private long getCurrentTimestamp() {
        String evaluationDateStr = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isNotBlank(evaluationDateStr)) {
            long currTime = LocalDate
                    .parse(evaluationDateStr, DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING)) //
                    .atTime(LocalTime.MAX) // end of date
                    .atZone(ZoneId.of("UTC")) //
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

    private boolean shouldExecute() {
        if (acc360Timeline == null) {
            log.info("account 360 timeline definition does not exist. Skip generating journey stages");
            return false;
        } else if (!hasAccountTimeLineChange() && !configuration.isShouldRebuild()) {
            log.info("No timeline diff table found in context (no new stream import)"
                    + " and not in rebuild mode, skip generating journey stages");
            return false;
        } else if (StringUtils.isBlank(accountBatchStoreTableName)) {
            log.info("No account batch store, skip generating journey stages");
            return false;
        } else if (CollectionUtils.size(journeyStages) <= 1) {
            log.info(
                    "There are no non-default journey stage configured, skip generating journey stages. Journey stages = {}",
                    journeyStages);
            return false;
        }
        return true;
    }

    private boolean isShortCutMode() {
        return getStringValueFromContext(JOURNEY_STAGE_TABLE_NAME) != null;
    }

    private void handleUpdatedAccount360TimelineTables(List<HdfsDataUnit> outputs) {
        HdfsDataUnit masterUnit = outputs.get(0);
        HdfsDataUnit diffUnit = outputs.get(1);
        // create updated (appended with new journey stage events) master/diff table
        String masterTableName = customerSpace.getTenantId() + "_" + NamingUtils.timestamp(TimelineProfile.name());
        String diffTableName = customerSpace.getTenantId() + "_" + NamingUtils.timestamp(TimelineProfile.name() + "Diff");
        metadataProxy.createTable(customerSpace.toString(), masterTableName, toTable(masterTableName, masterUnit));
        metadataProxy.createTable(customerSpace.toString(), diffTableName, toTable(diffTableName, diffUnit));
        // update table name in ctx
        updateValueInContext(TIMELINE_MASTER_TABLE_NAME, acc360Timeline.getTimelineId(), masterTableName);
        updateValueInContext(TIMELINE_DIFF_TABLE_NAME, acc360Timeline.getTimelineId(), diffTableName);
    }

    private void handleJourneyStageTable(HdfsDataUnit unit) {
        String journeyStageTableName = customerSpace.getTenantId() + "_"
                + NamingUtils.timestamp(AccountJourneyStage.name());
        metadataProxy.createTable(customerSpace.toString(), journeyStageTableName,
                toTable(journeyStageTableName, unit));
        putObjectInContext(JOURNEY_STAGE_TABLE_NAME, journeyStageTableName);
        linkJourneyStageTableToInactive();
    }

    private void linkTimelineMasterTablesToInactive() {
        Map<String, String> timelineMasterTables = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        log.info("Linking timeline master tables {} to inactive version {}", timelineMasterTables, inactive);
        if (MapUtils.isNotEmpty(timelineMasterTables)) {
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), timelineMasterTables,
                    TableRoleInCollection.TimelineProfile, inactive);
        }
    }

    private void linkJourneyStageTableToInactive() {
        if (StringUtils.isBlank(accountJourneyStageTableName)) {
            accountJourneyStageTableName = getStringValueFromContext(JOURNEY_STAGE_TABLE_NAME);
        }
        log.info("Linking account journey stage table name {} to inactive version {}", accountJourneyStageTableName,
                inactive);
        dataCollectionProxy.upsertTable(customerSpace.toString(), accountJourneyStageTableName, AccountJourneyStage,
                inactive);
    }

    private void updateValueInContext(String ctxKey, String key, String value) {
        Map<String, String> map = getMapObjectFromContext(ctxKey, String.class,
                String.class);
        map.put(key, value);
        putObjectInContext(ctxKey, map);
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

    private Table getAccount360TimeLineDiffTable() {
        Map<String, String> timelineTableNames = getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME, String.class,
                String.class);
        String tableName = MapUtils.emptyIfNull(timelineTableNames).get(account360TimelineId());
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        return metadataProxy.getTableSummary(customerSpace.toString(), tableName);
    }

    private Table getAccount360TimeLineMasterTable() {
        Map<String, String> tableNames = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        String tableName = tableNames.get(account360TimelineId());
        return metadataProxy.getTableSummary(customerSpace.toString(), tableName);
    }

    private TimeLine getAccount360TimeLine() {
        String id = account360TimelineId();
        List<TimeLine> timeLineList = configuration.getTimeLineList();
        if (CollectionUtils.isEmpty(timeLineList)) {
            return null;
        }

        return timeLineList.stream() //
                .filter(Objects::nonNull) //
                .filter(timeline -> id.equals(timeline.getTimelineId())) //
                .findAny() //
                .orElse(null);
    }

    private String account360TimelineId() {
        return TimeLineStoreUtils.contructTimelineId(configuration.getCustomer(),
                TimeLineStoreUtils.ACCOUNT360_TIMELINE_NAME);
    }

    private String getAccountJourneyStageTableName() {
        return getTableName(AccountJourneyStage, "account journey stage master store");
    }

    private String getAccountTableName() {
        return getTableName(Account.getBatchStore(), "account batch store");
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

    @Override
    protected Class<? extends AbstractSparkJob<JourneyStageJobConfig>> getJobClz() {
        return GenerateJourneyStageJob.class;
    }
}
