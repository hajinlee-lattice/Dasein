package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_ENRICH_WEBVISIT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedActivityStream;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedCatalog;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedWebVisit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.cdl.workflow.steps.merge.MatchUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(EnrichWebVisit.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class EnrichWebVisit extends BaseTransformWrapperStep<EnrichWebVisitStepConfiguration> {

    private static Logger log = LoggerFactory.getLogger(EnrichWebVisit.class);

    static final String BEAN_NAME = "enrichWebVisit";
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.StreamDateId.name());

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${cdl.pa.use.directplus}")
    private boolean useDirectPlus;

    private DataCollection.Version inactive;

    private List<ActivityImport> webVisitImports;
    //set of column names
    private final Set<String> webVisitImportColumnNames = new HashSet<>();
    private AtlasStream webVisitStream;


    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        if (BooleanUtils.isNotTrue(getObjectFromContext(IS_SSVI_TENANT, Boolean.class))) {
            log.info("tenant isn't ssvi tenant, will skip this step.");
            return null;
        }
        if (isShortCutMode()) {
            log.info("In shortcut mode, skip EnrichWebVisit.");
            return null;
        }
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        webVisitStream = getWebVisitStream();
        if (webVisitStream == null) {
            log.error("can't find webVisitStream, will skip this step.");
            return null;
        }
        String catalogTableName = getPathPatternCatalogTable();
        log.info("can't find catalog Table.");
        String matchedTableName = "";
        String latticeAccountTableName = "";
        String masterTableName = "";


        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        List<TransformationStepConfig> steps = new ArrayList<>();
        int mergeWebVisitStep = -1;
        if (BooleanUtils.isNotTrue(getObjectFromContext(IS_CDL_TENANT, Boolean.class))) {
            buildWebVisitImportColumnNames();
            if (!CollectionUtils.isEmpty(webVisitImports)) {
                log.info("find new webVisit Import, do mergeWebVisitImport and match.");
                List<String> importTables = webVisitImports.stream() //
                        .map(ActivityImport::getTableName) //
                        .collect(Collectors.toList());
                mergeWebVisitStep = concatAndMatchStreamImports(webVisitStream.getStreamId(), importTables, steps);
            }
            masterTableName = getActiveWebVisitBatchStoreTable(customerSpace.toString());
        } else {
            log.info("this tenant is cdl Tenant.");
            Map<String, String> rawStreamTableNames = getRawStreamTables(customerSpace.toString(), ConsolidatedActivityStream);
            matchedTableName = rawStreamTableNames.get(webVisitStream.getStreamId());
            Table latticeAccountTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.LatticeAccount,
                    inactive.complement());
            if (latticeAccountTable == null) {
                log.error("can't find latticeAccount Table, will skip this step.");
                return null;
            }
            latticeAccountTableName = latticeAccountTable.getName();
        }
        if ((StringUtils.isEmpty(matchedTableName) || mergeWebVisitStep == -1) && !configuration.isRebuildMode() && !hasCatalogImport(webVisitStream, configuration.getCatalogImports().keySet())) {
            log.info("no import data, no need rebuild, no new catalog import, will skip this step.");
            return null;
        }
        TransformationStepConfig enrichWebVisit = enrichWebVisitJob(mergeWebVisitStep, catalogTableName,
                matchedTableName, latticeAccountTableName, masterTableName);
        steps.add(enrichWebVisit);
        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String tableName = getFullTableName(ConsolidatedWebVisit.name());
        exportToS3AndAddToContext(tableName, SSVI_WEBVISIT_RAW_TABLE);
    }

    private boolean isShortCutMode() {
        String tableName = getStringValueFromContext(SSVI_WEBVISIT_RAW_TABLE);
        return !StringUtils.isEmpty(tableName);
    }

    private Integer concatAndMatchStreamImports(@NotNull String streamId, @NotNull List<String> importTables,
                                                @NotNull List<TransformationStepConfig> steps) {
        Preconditions.checkNotNull(webVisitStream, String.format("Stream %s is not in config", webVisitStream));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(webVisitStream.getMatchEntities()),
                String.format("Stream %s does not have match entities", streamId));

        // concat import
        steps.add(dedupAndConcatTables(null, false, importTables));
        addMatchStep(webVisitStream, webVisitImportColumnNames, steps, steps.size() - 1);
        return steps.size() - 1;
    }

    private void addMatchStep(@NotNull AtlasStream stream, @NotNull Set<String> importTableColumns,
                              @NotNull List<TransformationStepConfig> steps, int matchInputTableIdx) {
        String matchConfig;
        MatchInput baseMatchInput = getBaseMatchInput();
        // set stream type to source entity to get higher concurrency
        baseMatchInput.setSourceEntity(stream.getStreamType().name());
        // match entity
        matchConfig = getMatchConfig(baseMatchInput, importTableColumns);
        steps.add(match(matchInputTableIdx, null, matchConfig));
    }

    /**
     * if SSVI on, CDL on, using web visit raw stream table as input. so no need consider rebuild mode.
     * if SSVI on, CDL off, when rebuild/has new catalog import, input need import + consolidateWebVisit Table.
     * when rematch/replace, input is import
     */
    protected TransformationStepConfig enrichWebVisitJob(int mergeWebVisitStep, String catalogTableName,
                                                         String matchedTableName, String latticeAccountTableName,
                                                         String masterTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_ENRICH_WEBVISIT);
        EnrichWebVisitJobConfig config = new EnrichWebVisitJobConfig();

        int inputIdx = 0;
        if (mergeWebVisitStep != -1) {
            step.setInputSteps(Collections.singletonList(mergeWebVisitStep));
            config.matchedWebVisitInputIdx = inputIdx++;
        }
        if (StringUtils.isNotEmpty(catalogTableName)) {
            config.catalogInputIdx = inputIdx++;
            addBaseTables(step, catalogTableName);
        }
        if (StringUtils.isNotEmpty(matchedTableName)) {
            config.matchedWebVisitInputIdx = inputIdx++;
            addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), matchedTableName);
        }
        if (StringUtils.isNotEmpty(latticeAccountTableName)) {
            config.latticeAccountTableIdx = inputIdx++;
            addBaseTables(step, latticeAccountTableName);
        }
        if (StringUtils.isNotEmpty(masterTableName)) {
            config.masterInputIdx = inputIdx;
            addBaseTables(step, masterTableName);
        }

        config.selectedAttributes = getSelectedAttributes();
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        setTargetTable(step, ConsolidatedWebVisit.name());
        return step;
    }

    private TransformationStepConfig dedupAndConcatTables(String joinKey, boolean dedupSrc, List<String> tables) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        tables.forEach(tblName -> addBaseTables(step, tblName));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(dedupSrc);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig match(int inputStep, String matchTargetTable, String matchConfig) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(matchConfig);
        return step;
    }

    private String getMatchConfig(MatchInput baseMatchInput, Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setCustomSelection(getDataCloudAttrColumns());
        baseMatchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        baseMatchInput.setKeyMap(MatchUtils.getAccountMatchKeysAccount(columnNames, null, false, null));
        baseMatchInput.setPartialMatchEnabled(true);
        baseMatchInput.setTenant(new Tenant(customerSpace.toString()));
        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    private MatchInput getBaseMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setUseDirectPlus(useDirectPlus);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        return matchInput;
    }

    private AtlasStream getWebVisitStream() {
        AtlasStream stream = null;
        if (MapUtils.isEmpty(configuration.getActivityStreamMap())) {
            return null;
        }
        for (Map.Entry<String, AtlasStream> entry : configuration.getActivityStreamMap().entrySet()) {
            if (AtlasStream.StreamType.WebVisit.equals(entry.getValue().getStreamType())) {
                stream = entry.getValue();
                break;
            }
        }
        return stream;
    }

    private void buildWebVisitImportColumnNames() {
        webVisitImports = configuration.getStreamImports().get(webVisitStream.getStreamId());
        if (CollectionUtils.isEmpty(webVisitImports)) {
            return;
        }

        String[] importTables = webVisitImports.stream().map(ActivityImport::getTableName).toArray(String[]::new);
        Set<String> columns = getTableColumnNames(importTables);
        log.info("WebVisit has {} imports, {} total columns", webVisitImports.size(), columns.size());
        webVisitImportColumnNames.addAll(columns);
    }

    private Set<String> getTableColumnNames(String... tableNames) {
        // TODO add a batch retrieve API to optimize this
        return Arrays.stream(tableNames) //
                .flatMap(tableName -> metadataProxy //
                        .getTableColumns(customerSpace.toString(), tableName) //
                        .stream() //
                        .map(ColumnMetadata::getAttrName)) //
                .collect(Collectors.toSet());
    }

    private String getPathPatternCatalogTable() {
        Catalog pathPatternCatalog = null;
        String catalogTableName = "";
        for (Catalog catalog : configuration.getCatalogs()) {
            if (EntityType.WebVisitPathPattern.name().equals(catalog.getName())) {
                pathPatternCatalog = catalog;
                break;
            }
        }
        if (pathPatternCatalog == null) {
            return catalogTableName;
        }
        //catalogId -> catalogTableName
        Map<String, String> catalogTables = getActiveTables(customerSpace.toString(), BusinessEntity.Catalog,
                Collections.singletonList(pathPatternCatalog), Catalog::getCatalogId, ConsolidatedCatalog, inactive);
        if (MapUtils.isEmpty(catalogTables)) {
            catalogTables = getActiveTables(customerSpace.toString(), BusinessEntity.Catalog,
                    Collections.singletonList(pathPatternCatalog), Catalog::getCatalogId, ConsolidatedCatalog, inactive.complement());
        }
        if (MapUtils.isEmpty(catalogTables)) {
            return catalogTableName;
        }
        for (Map.Entry<String, String> entry : catalogTables.entrySet()) {
            catalogTableName = entry.getValue();
            break;
        }
        return catalogTableName;
    }

    private boolean hasCatalogImport(AtlasStream stream, Set<String> catalogsWithImports) {
        return stream.getDimensions().stream().filter(dim -> dim.getCatalog() != null)
                .anyMatch(dim -> catalogsWithImports.contains(dim.getCatalog().getCatalogId()));
    }

    private Map<String, String> getRawStreamTables(@NotNull String customerSpace, TableRoleInCollection role) {
        Map<String, String> rawStreamTableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace, role
                , inactive, Collections.singletonList(webVisitStream.getStreamId()));
        if (MapUtils.isEmpty(rawStreamTableNames)) {
            rawStreamTableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace, role,
                    inactive.complement(), Collections.singletonList(webVisitStream.getStreamId()));
        }
        return rawStreamTableNames;
    }

    /*-
     * table names in current active version for WebVisit
     */
    private String getActiveWebVisitBatchStoreTable(@NotNull String customerSpace) {
        if (configuration.isRematchMode() || configuration.isReplaceMode()) {
            return "";
        }
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace, ConsolidatedWebVisit, inactive);
        if (StringUtils.isEmpty(batchStoreTableName)) {
            batchStoreTableName = dataCollectionProxy.getTableName(customerSpace, ConsolidatedWebVisit, null);
        }
        return batchStoreTableName;
    }

    private <E> Map<String, String> getActiveTables(@NotNull String customerSpace, @NotNull BusinessEntity entity,
                                                    List<E> activityStoreEntities, Function<E, String> getUniqueIdFn,
                                                    TableRoleInCollection role, DataCollection.Version version) {
        if (CollectionUtils.isEmpty(activityStoreEntities)) {
            return Collections.emptyMap();
        }

        List<String> uniqueIds = activityStoreEntities.stream() //
                .filter(Objects::nonNull) //
                .map(getUniqueIdFn) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toList());
        Map<String, String> tables = dataCollectionProxy.getTableNamesWithSignatures(customerSpace, role, version,
                uniqueIds);
        log.info("Current {} tables for tenant {} are {}. StreamIds={}", entity, customerSpace, tables, uniqueIds);
        return tables;
    }

    private static ColumnSelection getDataCloudAttrColumns() {
        ColumnSelection selection = new ColumnSelection();
        List<String> cols = Arrays.asList("LE_GlobalUlt_salesUSD", "LE_DomUlt_SalesUSD", "LE_GlobalULt_EmployeeTotal",
                "LE_DomUlt_EmployeeTotal", "LDC_DUNS", "DOMESTIC_ULTIMATE_DUNS_NUMBER", "GLOBAL_ULTIMATE_DUNS_NUMBER",
                "LE_SIC_CODE", "LE_Site_NAICS_Code", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE",
                "LE_IS_PRIMARY_DOMAIN", "LDC_Domain", "LDC_Name", "LDC_Country", "LDC_State", "LDC_City",
                "LE_DNB_TYPE");
        selection.setColumns(cols.stream().map(Column::new).collect(Collectors.toList()));
        return selection;
    }

    private Map<String, String> getSelectedAttributes() {
        Map<String, String> selectedAttributes = new HashMap<>();
        if (BooleanUtils.isTrue(getObjectFromContext(IS_CDL_TENANT, Boolean.class))) {
            selectedAttributes.put(InterfaceName.AccountId.name(), "account_id");
        } else {
            selectedAttributes.put("LDC_DUNS", "account_id");
        }
        selectedAttributes.put("WebVisitDate", "visit_date");
        selectedAttributes.put("UserId", "user_id");
        selectedAttributes.put("WebVisitPageUrl", "page_url");
        selectedAttributes.put("SourceMedium", "source_medium");
        selectedAttributes.put("LE_GlobalUlt_salesUSD", "globalult_sales_usd");
        selectedAttributes.put("LE_DomUlt_SalesUSD", "domult_sales_usd");
        selectedAttributes.put("LE_GlobalULt_EmployeeTotal", "globalult_employee_total");
        selectedAttributes.put("LE_DomUlt_EmployeeTotal", "domult_employee_total");
        selectedAttributes.put("LDC_DUNS", "duns_number");
        selectedAttributes.put("DOMESTIC_ULTIMATE_DUNS_NUMBER", "domestic_ultimate_duns_number");
        selectedAttributes.put("GLOBAL_ULTIMATE_DUNS_NUMBER", "global_ultimate_duns_number");
        selectedAttributes.put("LE_SIC_CODE", "sic_code");
        selectedAttributes.put("LE_Site_NAICS_Code", "naics_code");
        selectedAttributes.put("LE_INDUSTRY", "industry");
        selectedAttributes.put("LE_EMPLOYEE_RANGE", "employee_range");
        selectedAttributes.put("LE_REVENUE_RANGE", "revenue_range");
        selectedAttributes.put("LE_IS_PRIMARY_DOMAIN", "is_primary_domain");
        selectedAttributes.put("LDC_Domain", "domain");
        selectedAttributes.put("LDC_Name", "company_name");
        selectedAttributes.put("LDC_Country", "country");
        selectedAttributes.put("LDC_State", "state");
        selectedAttributes.put("LDC_City", "city");
        selectedAttributes.put("LE_DNB_TYPE", "site_level");
        selectedAttributes.put("UrlCategories", "page_groups");
        selectedAttributes.put("UtmSource", "utm_source");
        selectedAttributes.put("UtmMedium", "utm_medium");
        selectedAttributes.put("UtmCampaign", "utm_campaign");
        selectedAttributes.put("UtmTerm", "utm_term");
        selectedAttributes.put("UtmContent", "utm_content");
        return selectedAttributes;
    }

    private String getFullTableName(String tablePrefix) {
        if (StringUtils.isEmpty(tablePrefix)) {
            return "";
        }
        return TableUtils.getFullTableName(tablePrefix, pipelineVersion);
    }

}
