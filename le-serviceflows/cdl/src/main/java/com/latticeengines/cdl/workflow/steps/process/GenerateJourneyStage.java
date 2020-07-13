package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.DataCollection;
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

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private String accountBatchStoreTableName;

    @Override
    protected JourneyStageJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        if (isShortCutMode()) {
            log.info("In short cut mode, skip generating journey stages");
            linkTimelineMasterTablesToInactive();
            return null;
        }

        // TODO get journey stage config
        accountBatchStoreTableName = getAccountTableName();
        acc360Timeline = getAccount360TimeLine();

        if (!shouldExecute()) {
            return null;
        }

        Table accountBatchStoreTable = metadataProxy.getTableSummary(customerSpace.toString(),
                accountBatchStoreTableName);
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
        inputs.add(accountBatchStoreTable.toHdfsDataUnit("AccountBatchStore"));
        JourneyStageJobConfig config = new JourneyStageJobConfig();
        // TODO consider to use evaluation date
        config.currentEpochMilli = getLongValueFromContext(PA_TIMESTAMP);
        config.masterAccountTimeLineIdx = 0;
        config.diffAccountTimeLineIdx = 1;
        config.masterAccountStoreIdx = 2;
        // TODO consider to do some ordering
        config.journeyStages = activityStoreProxy.getJourneyStages(customerSpace.toString());

        log.info("Current journey stage configuration = {}", JsonUtils.serialize(config.journeyStages));

        config.setInput(inputs);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        // [ master table, diff table ]
        List<HdfsDataUnit> outputs = result.getTargets();
        Preconditions.checkArgument(CollectionUtils.size(outputs) == 2,
                String.format("Journey stage spark job should output two tables (master, diff), got %d instead",
                        CollectionUtils.size(outputs)));
        // create updated timeline master/diff table and update corresponding contexts
        handleUpdatedAccount360TimelineTables(outputs);

        // link all tables to inactive version
        linkTimelineMasterTablesToInactive();

        // mark this step as completed
        putObjectInContext(JOURNEY_STAGE_GENERATED, true);
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
        }
        return true;
    }

    private boolean isShortCutMode() {
        return BooleanUtils.isTrue(getObjectFromContext(JOURNEY_STAGE_GENERATED, Boolean.class));
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

    private void linkTimelineMasterTablesToInactive() {
        Map<String, String> timelineMasterTables = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        log.info("Linking timeline master tables {} to inactive version {}", timelineMasterTables, inactive);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), timelineMasterTables,
                TableRoleInCollection.TimelineProfile, inactive);
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
