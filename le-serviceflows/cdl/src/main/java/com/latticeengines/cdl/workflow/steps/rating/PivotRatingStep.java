package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.PivotRatings;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;


@Component("pivotRatingStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PivotRatingStep extends RunSparkJob<GenerateRatingStepConfiguration, PivotRatingsConfig> {

    private static final Logger log = LoggerFactory.getLogger(PivotRatingStep.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected Class<PivotRatings> getJobClz() {
        return PivotRatings.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateRatingStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected PivotRatingsConfig configureJob(GenerateRatingStepConfiguration stepConfiguration) {
        Table ruleRawTable = getRawTable(RULE_RAW_RATING_TABLE_NAME);
        Table aiRawTable = getRawTable(AI_RAW_RATING_TABLE_NAME);
        List<String> inactiveEngines = getListObjectFromContext(ITERATION_INACTIVE_ENGINES, String.class);
        List<RatingModelContainer> modelContainers = //
                getListObjectFromContext(ITERATION_RATING_MODELS, RatingModelContainer.class);
        return createSparkJobConfig(aiRawTable, ruleRawTable, inactiveEngines, modelContainers);
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String resultTableName = NamingUtils.timestamp("PivotedRating");
        String pk = InterfaceName.AccountId.name();
        Table resultTable = toTable(resultTableName, pk, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);

        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        dataCollectionProxy.upsertTable(customerSpace.toString(), resultTableName, //
                TableRoleInCollection.PivotedRating, inactive);

        exportTableRoleToRedshift(resultTableName, PathUtils.toAvroGlob(resultTable.getExtracts().get(0).getPath()));
        exportToDynamo(resultTableName, PathUtils.toParquetOrAvroDir(resultTable.getExtracts().get(0).getPath()));

        putStringValueInContext(PIVOTED_RATINGS_TABLE_NAME, resultTableName);

        updateAttrRepo(resultTable);

        cleanupTemporaryTables();
    }

    private Table getRawTable(String contextKey) {
        Table tgtTble = null;
        String tblName = getStringValueFromContext(contextKey);
        if (StringUtils.isNotBlank(tblName)) {
            tgtTble = metadataProxy.getTable(customerSpace.toString(), tblName);
            if (tgtTble == null) {
                log.warn("Cannot find " + contextKey + " table " + tblName);
            }
        } else {
            log.info("Did not find " + contextKey + " in workflow context.");
        }
        return tgtTble;
    }

    private PivotRatingsConfig createSparkJobConfig(Table aiRawTable, Table ruleRawTable, //
                                                    List<String> inactiveEngines, List<RatingModelContainer> modelContainers) {
        Map<String, String> modelIdToEngineIdMap = new HashMap<>();
        List<String> evModelIds = new ArrayList<>();
        List<String> aiModelIds = new ArrayList<>();
        for (RatingModelContainer modelContainer : modelContainers) {
            String engineId = modelContainer.getEngineSummary().getId();
            String modelId = modelContainer.getModel().getId();
            modelIdToEngineIdMap.put(modelId, RatingEngine.toRatingAttrName(engineId));
            RatingEngineType ratingEngineType = modelContainer.getEngineSummary().getType();
            if (RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                    || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType)) {
                aiModelIds.add(modelId);
                AIModel aiModel = (AIModel) modelContainer.getModel();
                if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                    evModelIds.add(modelId);
                }
            }
        }

        PivotRatingsConfig config = new PivotRatingsConfig();
        config.setIdAttrsMap(modelIdToEngineIdMap);
        config.setEvModelIds(evModelIds);
        config.setAiModelIds(aiModelIds);
        List<DataUnit> inputUnits = new ArrayList<>();
        if (aiRawTable != null) {
            config.setAiSourceIdx(inputUnits.size());
            inputUnits.add(aiRawTable.toHdfsDataUnit("ai"));
        }
        if (ruleRawTable != null) {
            config.setRuleSourceIdx(inputUnits.size());
            inputUnits.add(ruleRawTable.toHdfsDataUnit("rule"));
        }
        Table inactiveTable = null;
        if (CollectionUtils.isNotEmpty(inactiveEngines)) {
            config.setInactiveEngineIds(inactiveEngines);
            DataCollection.Version version = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            inactiveTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                    BusinessEntity.Rating.getServingStore(), version);
        }
        if (inactiveTable != null) {
            config.setInactiveSourceIdx(inputUnits.size());
            inputUnits.add(inactiveTable.toHdfsDataUnit("inactive"));
        }
        config.setInput(inputUnits);
        return config;
    }

    private void exportTableRoleToRedshift(String tableName, String avroGlob) {
        TableRoleInCollection tableRole = TableRoleInCollection.PivotedRating;
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(avroGlob);
        config.setUpdateMode(false);
        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

    private void exportToDynamo(String tableName, String avroDir) {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(avroDir);
        config.setPartitionKey(InterfaceName.AccountId.name());
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private void cleanupTemporaryTables() {
        deleteTableNameInContext(RULE_RAW_RATING_TABLE_NAME);
        deleteTableNameInContext(AI_RAW_RATING_TABLE_NAME);
        deleteTableNameInContext(FILTER_EVENT_TARGET_TABLE_NAME);
        deleteTableNameInContext(SCORING_RESULT_TABLE_NAME);

        deleteTableInContext(PREMATCH_UPSTREAM_EVENT_TABLE);
        deleteTableInContext(EVENT_TABLE);
    }

    private void deleteTableNameInContext(String contextKey) {
        String tableName = getStringValueFromContext(contextKey);
        if (StringUtils.isNotBlank(tableName)) {
            log.info("Deleting temporary table " + tableName);
            metadataProxy.deleteTable(customerSpace.toString(), tableName);
        }
    }

    private void deleteTableInContext(String contextKey) {
        Table table = getObjectFromContext(contextKey, Table.class);
        if (table != null) {
            log.info("Deleting temporary table " + table.getName());
            metadataProxy.deleteTable(customerSpace.toString(), table.getName());
        }
    }

    private void updateAttrRepo(Table ratingTable) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo != null) {
            log.info("Update Rating attrs in attr repo.");
            attrRepo.appendServingStore(BusinessEntity.Rating, ratingTable);
        }
    }

}
