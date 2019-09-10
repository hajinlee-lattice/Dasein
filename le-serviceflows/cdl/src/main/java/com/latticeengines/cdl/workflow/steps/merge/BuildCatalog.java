package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BuildCatalogStepConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(BuildCatalog.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildCatalog extends BaseMergeImports<BuildCatalogStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildCatalog.class);

    static final String BEAN_NAME = "buildCatalog";

    private static final String CATALOG_TABLE_PREFIX_FORMAT = "Catalog_%s";

    @Inject
    private EMREnvService emrEnvService;

    // tableName, idx in inputTableNames (sorted by last modified time desc)
    private Map<String, Integer> inputTableIdxMap = new HashMap<>();
    // catalogName -> tableName used to build catalog
    private Map<String, String> inputCatalogTables = new HashMap<>();
    // catalogName -> original file name of used table
    private Map<String, String> inputCatalogFiles = new HashMap<>();

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        // TODO add shortcut mode (maybe not worth doing)
        buildInputTableIdxMap();
        selectCatalogImport();
        List<String> tableNames = buildCatalogBatchStore();
        exportToS3AndAddToContext(tableNames, CATALOG_TABLE_NAME);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        // nothing for now, placeholder for future enhancements
        return null;
    }

    /*-
     * Setup all catalog batch store tables.
     * If there is a new import for certain catalog, use that import to replace existing table
     * Otherwise use the existing table (link from active version)
     */
    private List<String> buildCatalogBatchStore() {
        Map<String, String> activeTables = configuration.getCatalogTables();

        // clone and rename catalog import tables
        Map<String, String> finalCatalogTables = new HashMap<>(cloneCatalogImports());
        if (MapUtils.isNotEmpty(activeTables)) {
            // only add current table if there is no import for specific catalog
            activeTables.forEach(finalCatalogTables::putIfAbsent);
        }
        log.info("ConsolidatedCatalog tables = {}, Current active catalog tables = {}", finalCatalogTables,
                activeTables);

        // link all tables
        if (MapUtils.isNotEmpty(finalCatalogTables)) {
            // use catalogName as signature
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), finalCatalogTables, batchStore,
                    inactive);
        }
        return new ArrayList<>(finalCatalogTables.values());
    }

    /*-
     * Clone catalog import tables and returns a map of [ CatalogName -> Cloned TableName ]
     */
    private Map<String, String> cloneCatalogImports() {
        // rename catalog import tables
        Map<String, String> catalogTables = inputCatalogTables //
                .entrySet() //
                .stream() //
                .collect(Collectors.toMap(Map.Entry::getKey, e -> tableName(e.getKey())));

        log.info("Cloning tables for catalog imports. Source tables={}, Dest tables={}", inputCatalogTables, catalogTables);
        for (String catalogName : inputCatalogTables.keySet()) {
            String srcTableName = inputCatalogTables.get(catalogName);
            String dstTableName = catalogTables.get(catalogName);

            // clone table from catalog import
            Table srcTable = inputTables.get(inputTableIdxMap.get(srcTableName));
            String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
            queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
            Table dstTable = TableCloneUtils //
                    .cloneDataTable(yarnConfiguration, customerSpace, dstTableName, srcTable, queue);
            metadataProxy.createTable(customerSpace.toString(), dstTableName, dstTable);
        }

        return catalogTables;
    }

    /*-
     * Generate table name for target catalog
     */
    private String tableName(@NotNull String catalogName) {
        return NamingUtils.timestamp(String.format(CATALOG_TABLE_PREFIX_FORMAT, catalogName));
    }

    /*-
     * Choose the latest import for each catalog. Log warning if some catalog has more than 1 imports.
     */
    private void selectCatalogImport() {
        Map<String, List<CatalogImport>> catalogImports = configuration.getCatalogImports();
        if (MapUtils.isEmpty(catalogImports)) {
            return;
        }

        catalogImports.forEach((catalogName, pairs) -> {
            if (CollectionUtils.isEmpty(pairs)) {
                log.warn("Found empty catalog table pairs in config for catalog {}", catalogName);
                return;
            }

            // add warning if more than 1 import
            if (pairs.size() > 1) {
                String msg = String.format("More than one imports for catalog %s, only the last one will be used",
                        catalogName);
                addToListInContext(PROCESS_ANALYTICS_WARNING_KEY, msg, String.class);
            }

            // select last valid catalog import (smallest idx in inputTableNames)
            Optional<CatalogImport> result = pairs.stream() //
                    .filter(Objects::nonNull) //
                    .filter(imp -> inputTableIdxMap.containsKey(imp.getTableName())) //
                    .min(Comparator.comparingInt(imp -> inputTableIdxMap.get(imp.getTableName())));
            if (!result.isPresent()) {
                log.warn("No valid import for catalog {}. catalogTablePairs={}", catalogName, pairs);
                return;
            }

            // update maps with selected import
            String tableName = result.get().getTableName();
            String origFileName = result.get().getOriginalFilename();
            inputCatalogTables.put(catalogName, tableName);
            inputCatalogFiles.put(catalogName, origFileName);
        });
        saveCatalogFilenames(inputCatalogFiles);
    }

    private void saveCatalogFilenames(Map<String, String> catalogFilenames) {
        if (MapUtils.isEmpty(catalogFilenames)) {
            return;
        }
        log.info("Saving original catalog file map to DataCollectionStatus. CatalogFileMap={}", catalogFilenames);
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status.setOrigCatalogFileMap(catalogFilenames);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    private void buildInputTableIdxMap() {
        if (CollectionUtils.isEmpty(inputTableNames)) {
            return;
        }

        for (int i = 0; i < inputTableNames.size(); i++) {
            inputTableIdxMap.put(inputTableNames.get(i), i);
        }
    }
}
