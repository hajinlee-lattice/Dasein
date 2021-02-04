package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.PUBLISH_TO_ELASTICSEARCH;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component(FinishActivityStreamProcessing.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class FinishActivityStreamProcessing extends BaseWorkflowStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishActivityStreamProcessing.class);
    private static final String PARTITION_KEY_NAME = InterfaceName.PartitionKey.name();
    private static final String SORT_KEY_NAME = InterfaceName.SortKey.name();
    private static final String ENTITY_CLASS_NAME = GenericTableActivity.class.getCanonicalName();

    static final String BEAN_NAME = "finishActivityStreamProcessing";

    @Inject
    private BatonService batonService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;



    @Override
    public void execute() {
        publishTimelineDiffTablesToDynamo();
        registerDataUnits();
        publishToElasticSearch();
    }

    public void publishTimelineDiffTablesToDynamo() {
        if (!shouldPublishTimelineToDynamo()) {
            log.info("Skip publishing timeline diff table to dynamo. Account360 enabled = {}, skipPublishDynamo = {}",
                    account360Enabled(), skipPublishDynamo);
            return;
        }

        Map<String, String> timelineTableNames = getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME, String.class,
                String.class);
        if (MapUtils.isEmpty(timelineTableNames)) {
            log.info("No timeline diff table found in context, skip publishing to dynamo");
            return;
        }

        log.info("Publishing timeline diff tables {} to dynamo", timelineTableNames);
        timelineTableNames.values().forEach(tableName -> {
            exportToDynamo(tableName);
            addToListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);
        });
    }

    void publishToElasticSearch() {
        boolean publishToES = elasticSearchEnabled();
        if (!publishToES) {
            log.info("skip publishing to elastic search as false publish to elastic search in {}",
                    configuration.getCustomerSpace());
            return ;
        }
        DataUnit unit = dataUnitProxy.getByNameAndTypeInCache(configuration.getCustomerSpace().toString(),
                TableRoleInCollection.TimelineProfile.name(), DataUnit.StorageType.ElasticSearch);
        Boolean rebuild = getObjectFromContext(TIMELINE_REBUILD, Boolean.class);
        boolean isNull = unit == null;
        boolean isRebuild = Boolean.TRUE.equals(rebuild);
        log.info("data unit is null: {}, rebuild {}", isNull, isRebuild);
        boolean fromFullTimeMap = isNull || isRebuild;
        Map<String, String> timelineTableNames = fromFullTimeMap ?
                getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class, String.class) :
                getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME, String.class, String.class);

        if (MapUtils.isEmpty(timelineTableNames)) {
            log.info("empty time line table map");
            return ;
        }
        // get time line version
        String streamId =  TimeLineStoreUtils.contructTimelineId(configuration.getCustomerSpace().toString(),
                TimeLineStoreUtils.ACCOUNT360_TIMELINE_NAME);
        String tableName = timelineTableNames.get(streamId);
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, String> timelineVersion = MapUtils.emptyIfNull(status.getTimelineVersionMap());
        String version = timelineVersion.get(streamId);
        if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(version)) {
            log.info("table name {}, version {}", tableName, version);
            exportToES(tableName, version);
            log.info("remove table {} from temporary list if needed", tableName);
            removeFromListInContext(TEMPORARY_CDL_TABLES, tableName, String.class);

        } else {
            log.info("empty table name or version");
        }
    }

    private void exportToDynamo(String tableName) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        config.setPartitionKey(PARTITION_KEY_NAME);
        config.setSortKey(SORT_KEY_NAME);
        addToListInContext(TIMELINE_RAWTABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private void exportToES(String tableName, String version) {
        ElasticSearchExportConfig config = new ElasticSearchExportConfig();
        config.setTableName(tableName);
        config.setTableRoleInCollection(TableRoleInCollection.TimelineProfile);
        config.setSignature(version);
        addToListInContext(TABLES_GOING_TO_ES, config, ElasticSearchExportConfig.class);
    }

    private void registerDataUnits() {
        Map<String, String> timelineMasterTables = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        if (MapUtils.isEmpty(timelineMasterTables)) {
            log.info("No timeline master table found in context, skip create dataUnit");
            return;
        }
        log.info("create timeline master tables {} to datanit", timelineMasterTables);
        timelineMasterTables.values().forEach(this::registerSingleDataUnit);
    }

    private void registerSingleDataUnit(String tableName) {
        String customerSpace = configuration.getCustomerSpace().toString();
        DynamoDataUnit unit = new DynamoDataUnit();
        unit.setTenant(CustomerSpace.shortenCustomerSpace(customerSpace));
        unit.setEntityClass(ENTITY_CLASS_NAME);
        unit.setName(tableName);
        unit.setPartitionKey(PARTITION_KEY_NAME);
        unit.setSortKey(SORT_KEY_NAME);
        unit.setSignature(configuration.getTimelineSignature());
        DataUnit created = dataUnitProxy.create(customerSpace, unit);
        log.info("Registered DataUnit: " + JsonUtils.pprint(created));
    }

    private boolean account360Enabled() {
        return batonService.isEnabled(configuration.getCustomerSpace(), ENABLE_ACCOUNT360);
    }

    private boolean shouldPublishTimelineToDynamo() {
        return !skipPublishDynamo && account360Enabled();
    }

    private boolean elasticSearchEnabled() {
        return batonService.isEnabled(configuration.getCustomerSpace(), PUBLISH_TO_ELASTICSEARCH);
    }
}
