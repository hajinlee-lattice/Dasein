package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_UPSERT_TXMFR;
import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Replace;
import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Upsert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.CatalogImport;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BuildCatalogStepConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(BuildCatalog.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildCatalog extends BaseMergeImports<BuildCatalogStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildCatalog.class);

    static final String BEAN_NAME = "buildCatalog";

    private static final String CATALOG_MERGED_IMPORT_TABLE_PREFIX_FORMAT = "Catalog_MergedImports_%s";
    private static final String CATALOG_TABLE_PREFIX_FORMAT = "Catalog_%s";

    // catalogName -> final catalog batch store table name prefix
    private Map<String, String> finalCatalogTablePrefixes = new HashMap<>();

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        setupCatalogImportFileMap();
        // TODO add shortcut model
    }

    @Override
    protected void onPostTransformationCompleted() {
        // not executing base method since no diff report for now
        // TODO add diff report if necessary later
        List<String> tableNames = buildCatalogBatchStore();
        exportToS3AndAddToContext(tableNames, CATALOG_TABLE_NAME);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        // merge catalog imports and upsert to master if in upsert mode and active batch
        // store exists
        List<TransformationStepConfig> steps = new ArrayList<>();
        configuration.getCatalogImports() //
                .forEach((catalogName, catalogImports) -> {
                    List<String> importTableNames = catalogImports.stream().map(CatalogImport::getTableName)
                            .collect(Collectors.toList());
                    mergeAndUpsertCatalogImports(catalogName, importTableNames, steps);
                });
        if (CollectionUtils.isEmpty(steps)) {
            return null;
        }

        request.setSteps(steps);
        return request;
    }

    private void mergeAndUpsertCatalogImports(@NotNull String catalogName, @NotNull List<String> importTables,
            @NotNull List<TransformationStepConfig> steps) {
        if (CollectionUtils.isEmpty(importTables)) {
            return;
        }
        DataFeedTask.IngestionBehavior behavior = configuration.getIngestionBehaviors().get(catalogName);
        String primaryKey = configuration.getPrimaryKeyColumns().get(catalogName);
        String activeCatalogTable = configuration.getCatalogTables().get(catalogName);
        // check ingestion behavior and primary key
        if (behavior != Upsert && behavior != Replace) {
            log.error("Unsupported ingestion behavior {} for catalog {}", behavior, catalogName);
            throw new UnsupportedOperationException(
                    String.format("Do not support ingestion behavior %s in catalog", behavior));
        }
        if (behavior == Upsert && StringUtils.isBlank(primaryKey)) {
            throw new IllegalArgumentException(
                    String.format("Primary key column should be set when catalog %s is in Upsert mode", catalogName));
        }

        // generate merge & upsert configs
        String mergedImportTablePrefix = String.format(CATALOG_MERGED_IMPORT_TABLE_PREFIX_FORMAT, catalogName);
        TransformationStepConfig mergeImportStep = mergeImports(importTables, mergedImportTablePrefix, primaryKey);
        steps.add(mergeImportStep);

        log.info(
                "Merging catalog imports (prefix={}). CatalogName={}, ImportTables={}, IngestionBehavior={}, PrimaryKey={}, ActiveTable={}",
                mergedImportTablePrefix, catalogName, importTables, behavior, primaryKey, activeCatalogTable);

        if (StringUtils.isNotBlank(activeCatalogTable) && behavior == Upsert) {
            String catalogTablePrefix = tableName(catalogName);
            // has active batch store, need to upsert table
            steps.add(upsertBatchStore(steps.size() - 1, activeCatalogTable, catalogTablePrefix, primaryKey));
            finalCatalogTablePrefixes.put(catalogName, catalogTablePrefix);
            log.info("Upsert catalog {} to master, prefix={}", catalogName, catalogTablePrefix);
        } else {
            // merged import table is the final result
            finalCatalogTablePrefixes.put(catalogName, mergedImportTablePrefix);
        }
    }

    private TransformationStepConfig upsertBatchStore(int mergeStepIdx, @NotNull String activeTableName,
            @NotNull String catalogTablePrefix, @NotNull String primaryKey) {
        TransformationStepConfig step = new TransformationStepConfig();
        setTargetTable(step, activeTableName);
        // use the merged imports result to upsert
        step.setInputSteps(Collections.singletonList(mergeStepIdx));
        step.setTransformer(TRANSFORMER_UPSERT_TXMFR);
        setTargetTable(step, catalogTablePrefix);

        // TODO make sure this is correct
        UpsertConfig config = new UpsertConfig();
        config.setColsFromLhs(Collections.singletonList(InterfaceName.CDLCreatedTime.name()));
        config.setNotOverwriteByNull(true);
        config.setJoinKey(primaryKey);
        config.setSwitchSides(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig mergeImports(@NotNull List<String> importTables,
            @NotNull String mergedImportTablePrefix, String primaryKey) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        importTables.forEach(tblName -> addBaseTables(step, tblName));
        setTargetTable(step, mergedImportTablePrefix);

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        if (StringUtils.isNotBlank(primaryKey)) {
            config.setJoinKey(primaryKey);
        }
        config.setAddTimestamps(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    /*-
     * Setup all catalog batch store tables.
     * If there is a new import for certain catalog, use that import to replace existing table
     * Otherwise use the existing table (link from active version)
     */
    private List<String> buildCatalogBatchStore() {
        if (MapUtils.isEmpty(finalCatalogTablePrefixes)) {
            return Collections.emptyList();
        }

        Map<String, String> finalCatalogTables = new HashMap<>(finalCatalogTablePrefixes.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), TableUtils.getFullTableName(entry.getValue(), pipelineVersion))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        // add tables without imports
        configuration.getCatalogTables().forEach(finalCatalogTables::putIfAbsent);
        log.info("Building catalog tables, tables={}, pipelineVersion={}", finalCatalogTables, pipelineVersion);

        // link all tables and use catalogName as signature
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), finalCatalogTables, batchStore,
                inactive);
        return new ArrayList<>(finalCatalogTables.values());
    }

    /*-
     * Generate table name for target catalog batch store
     */
    private String tableName(@NotNull String catalogName) {
        return NamingUtils.timestamp(String.format(CATALOG_TABLE_PREFIX_FORMAT, catalogName));
    }

    private void setupCatalogImportFileMap() {
        if (MapUtils.isEmpty(configuration.getCatalogImports())) {
            return;
        }

        // catalogName -> list of original import filenames
        Map<String, List<String>> originalInputFiles = new HashMap<>();
        configuration.getCatalogImports()
                .forEach((catalogName, catalogImports) -> originalInputFiles.put(catalogName, catalogImports.stream() //
                        .filter(Objects::nonNull) //
                        .map(CatalogImport::getOriginalFilename) //
                        .filter(StringUtils::isNotBlank) //
                        .collect(Collectors.toList())));
        log.info("Saving original catalog file map to DataCollectionStatus. CatalogFileMap={}", originalInputFiles);
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (status != null) {
            status.setOrigCatalogFileMap(originalInputFiles);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        }
    }
}
