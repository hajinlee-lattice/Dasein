package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidateWebVisit;
import static com.latticeengines.domain.exposed.query.BusinessEntity.ActivityStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.cdl.workflow.steps.process.GenerateTimeLine;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.EnrichWebVisitJob;

@Component(EnrichWebVisit.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class EnrichWebVisit extends RunSparkJob<EnrichWebVisitSparkStepConfiguration, EnrichWebVisitJobConfig> {

    private static Logger log = LoggerFactory.getLogger(EnrichWebVisit.class);

    static final String BEAN_NAME = "enrichWebVisit";

    private static final TypeReference<Map<String, Map<String, DimensionMetadata>>> METADATA_MAP_TYPE = new TypeReference<Map<String, Map<String, DimensionMetadata>>>() {
    };

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private ActivityStoreProxy activityStoreProxy;
    @Inject
    private DataUnitProxy dataUnitProxy;

    private List<String> needRebuildStreamIds;
    private Map<String, String> relinkedTables = new HashMap<>();
    private DataCollection.Version inactive;

    @Override
    protected Class<? extends AbstractSparkJob<EnrichWebVisitJobConfig>> getJobClz() {
        return EnrichWebVisitJob.class;
    }

    @Override
    protected EnrichWebVisitJobConfig configureJob(EnrichWebVisitSparkStepConfiguration stepConfiguration) {
        EnrichWebVisitJobConfig config = new EnrichWebVisitJobConfig();
        if (Boolean.FALSE.equals(getObjectFromContext(IS_SSVI_TENANT, Boolean.class))) {
             return null;
        }
        if (Boolean.TRUE.equals(getObjectFromContext(IS_CDL_TENANT, Boolean.class))) {
            config.matchedRawStreamTables = getMapObjectFromContext(ENTITY_MATCH_STREAM_TARGETTABLE, String.class,
                    String.class);
        } else {
            config.matchedRawStreamTables = getMapObjectFromContext(SSVI_MATCH_STREAM_TARGETTABLE, String.class, String.class);
        }
        Map<String, AtlasStream> webVisitStreamMap = getWebVisitStreamMap();
        if (MapUtils.isEmpty(webVisitStreamMap)) {
            return null;
        }
        config.needRebuildTableStreamIds = needRebuildStreamIds;
        config.masterTableMap = getActiveWebVisitBatchStoreTables(configuration.getCustomer(),
                new ArrayList<>(webVisitStreamMap.values()));
        if (configuration.isRebuildMode() && MapUtils.isNotEmpty(config.masterTableMap)) {
            config.needRebuildTableStreamIds.addAll(config.masterTableMap.keySet());
        }
        // set dimensions
        Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap =
                getTypedObjectFromContext(STREAM_DIMENSION_METADATA_MAP, METADATA_MAP_TYPE);
        if (dimensionMetadataMap == null) {
            dimensionMetadataMap = activityStoreProxy.getDimensionMetadata(customerSpace.toString(), null,
                    false);
        }
        if (MapUtils.isEmpty(dimensionMetadataMap)) {
            log.info("can't find the DimensionMetadata, will skip SSVI EnrichWebVisit Step.");
            return null;
        }
        config.dimensionMetadataMap = dimensionMetadataMap;
        List<DataUnit> inputs = new ArrayList<>();
        toDataUnits(new ArrayList<>(config.matchedRawStreamTables.values()), config.matchedRawStreamInputIdx, inputs);
        toDataUnits(new ArrayList<>(config.masterTableMap.values()), config.masterInputIdx, inputs);
        config.setInput(inputs);
        Set<String> targetTableStreamIds = new HashSet<>();
        if (MapUtils.isNotEmpty(config.matchedRawStreamTables)) {
            targetTableStreamIds.addAll(config.matchedRawStreamTables.keySet());
        }
        if (CollectionUtils.isNotEmpty(config.needRebuildTableStreamIds)) {
            targetTableStreamIds.addAll(config.needRebuildTableStreamIds);
        }
        if (targetTableStreamIds.size() <= 0) {
            return null;
        }
        config.targetTableStreamIds = targetTableStreamIds;
        config.targetNum = targetTableStreamIds.size();

        Map<Integer, DataUnit.DataFormat> specialTargets = new HashMap<>();
        for (int i=0; i < targetTableStreamIds.size(); i++) {
            specialTargets.put(i, DataUnit.DataFormat.PARQUET);
        }
        config.setSpecialTargets(specialTargets);
        config.selectedAttributes = getSelectedAttributes();
        relinkedTables =
                config.masterTableMap.entrySet().stream().filter(entry -> !targetTableStreamIds.contains(entry.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Integer> webvisitTableOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(webvisitTableOutputIdx),
                "webVisitTable output index map should not be empty here");
        Map<String, Table> tables = new HashMap<>();
        Map<String, String>  tableNames = new HashMap<>();
        webvisitTableOutputIdx.forEach((streamId, outputIdx) -> {
            String tableName = streamId + "_" + NamingUtils.timestamp(ConsolidateWebVisit.name() + "Diff");
            Table table = toTable(tableName, result.getTargets().get(outputIdx));
            log.info("Create timeline diff table {} for timeline ID {}", tableName, streamId);
            metadataProxy.createTable(configuration.getCustomer(), tableName, table);
            tableNames.put(streamId, tableName);
            tables.put(streamId, table);
            exportToS3(table);
            dataUnitProxy.registerAthenaDataUnit(configuration.getCustomer(), tableName);
        });
        tableNames.putAll(relinkedTables);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), tableNames,
                ConsolidateWebVisit, inactive);
    }

    private Map<String, AtlasStream> getWebVisitStreamMap() {
        Map<String, AtlasStream> webVisitStreamMap = new HashMap<>();
        needRebuildStreamIds = new ArrayList<>();
        if (MapUtils.isEmpty(configuration.getActivityStreamMap())) {
            return webVisitStreamMap;
        }
        configuration.getActivityStreamMap().forEach((streamId, stream) -> {
            if (AtlasStream.StreamType.WebVisit.equals(stream.getStreamType())) {
                webVisitStreamMap.put(streamId, stream);
                if (hasCatalogImport(stream, configuration.getCatalogImports().keySet())) {
                    needRebuildStreamIds.add(streamId);
                }
            }
        });
        return webVisitStreamMap;
    }

    private boolean hasCatalogImport(AtlasStream stream, Set<String> catalogsWithImports) {
        return stream.getDimensions().stream().filter(dim -> dim.getCatalog() != null)
                .anyMatch(dim -> catalogsWithImports.contains(dim.getCatalog().getCatalogId()));
    }

    private List<HdfsDataUnit> toDataUnits(List<String> tableNames, Map<String, Integer> inputIdx,
                                           List<DataUnit> inputs) {
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        }

        return tableNames.stream() //
                .map(name -> {
                    inputIdx.put(name, inputs.size());
                    return metadataProxy.getTable(configuration.getCustomer(), name);
                }) //
                .map(table -> {
                    HdfsDataUnit du = table.partitionedToHdfsDataUnit("RawStream",
                            Collections.singletonList(InterfaceName.StreamDateId.name()));
                    inputs.add(du);
                    return du;
                }) //
                .collect(Collectors.toList());
    }

    /*-
     * table names in current active version for WebVisit
     */
    private Map<String, String> getActiveWebVisitBatchStoreTables(@NotNull String customerSpace,
                                                              List<AtlasStream> streams) {
        if (configuration.isRematchMode() || configuration.isReplaceMode()) {
            return new HashMap<>();
        }
        Map<String, String> webVisitBatchStoreTables = getMapObjectFromContext(SSVI_WEBVISIT_RAW_TABLES, String.class
                , String.class);
        if (MapUtils.isNotEmpty(webVisitBatchStoreTables)) {
            return webVisitBatchStoreTables;
        }
        return getActiveTables(customerSpace, ActivityStream, streams, AtlasStream::getStreamId,
                ConsolidateWebVisit);
    }

    private <E> Map<String, String> getActiveTables(@NotNull String customerSpace, @NotNull BusinessEntity entity,
                                                    List<E> activityStoreEntities, Function<E, String> getUniqueIdFn, TableRoleInCollection role) {
        if (CollectionUtils.isEmpty(activityStoreEntities)) {
            return Collections.emptyMap();
        }

        List<String> uniqueIds = activityStoreEntities.stream() //
                .filter(Objects::nonNull) //
                .map(getUniqueIdFn) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toList());
        Map<String, String> tables = dataCollectionProxy.getTableNamesWithSignatures(customerSpace, role, null,
                uniqueIds);
        log.info("Current {} tables for tenant {} are {}. StreamIds={}", entity, customerSpace, tables, uniqueIds);
        return tables;
    }

    private static Map<String, String> getSelectedAttributes() {
        Map<String, String> selectedAttributes = new HashMap<>();
        selectedAttributes.put("AccountId", "account_id");
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
        return selectedAttributes;
    }

}
