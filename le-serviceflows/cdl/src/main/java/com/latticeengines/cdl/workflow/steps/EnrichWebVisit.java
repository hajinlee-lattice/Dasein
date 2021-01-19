package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidateWebVisit;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedCatalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
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

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private DataUnitProxy dataUnitProxy;

    private DataCollection.Version inactive;

    @Override
    protected Class<? extends AbstractSparkJob<EnrichWebVisitJobConfig>> getJobClz() {
        return EnrichWebVisitJob.class;
    }

    /**
     * if SSVI on, CDL on, using web visit raw stream table as input. so no need consider rebuild mode.
     * if SSVI on, CDL off, when rebuild/has new catalog import, input need import + consolidateWebVisit Table.
     * when rematch/replace, input is import
     * @param stepConfiguration
     * @return
     */
    @Override
    protected EnrichWebVisitJobConfig configureJob(EnrichWebVisitSparkStepConfiguration stepConfiguration) {
        if (Boolean.FALSE.equals(getObjectFromContext(IS_SSVI_TENANT, Boolean.class))) {
             return null;
        }
        EnrichWebVisitJobConfig config = new EnrichWebVisitJobConfig();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        List<DataUnit> inputs = new ArrayList<>();
        AtlasStream webVisitStream = getWebVisitStream();
        if (webVisitStream == null) {
            return null;
        }
        String catalogTableName = getPathPatternCatalogTable();
        if (StringUtils.isEmpty(catalogTableName)) {
            return null;
        }
        config.catalogInputIdx = toDataUnit(catalogTableName, "Catalog", inputs);
        String matchedTableName;
        if (Boolean.TRUE.equals(getObjectFromContext(IS_CDL_TENANT, Boolean.class))) {
            Map<String, String> rawStreamTableNames = getMapObjectFromContext(RAW_ACTIVITY_STREAM_TABLE_NAME,
                    String.class, String.class);
            matchedTableName = rawStreamTableNames.get(webVisitStream.getStreamId());
            config.matchedWebVisitInputIdx = toPartitionedDataUnit(matchedTableName, inputs);
            Table latticeAccountTable = dataCollectionProxy.getTable(configuration.getCustomer(), TableRoleInCollection.LatticeAccount,
                    inactive.complement());
            if (latticeAccountTable == null) {
                log.error("can't find latticeAccount Table, will skip this step.");
                return null;
            }
            config.latticeAccountTableIdx = inputs.size();
            inputs.add(latticeAccountTable.toHdfsDataUnit("LatticeAccount"));
        } else {
            matchedTableName = getStringValueFromContext(SSVI_MATCH_STREAM_TARGETTABLE);
            config.matchedWebVisitInputIdx = toDataUnit(matchedTableName, "MatchedTable", inputs);
            String masterTableName = getActiveWebVisitBatchStoreTable(configuration.getCustomer());
            config.masterInputIdx = toDataUnit(masterTableName, "MasterTable", inputs);
        }
        config.setInput(inputs);
        config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        config.selectedAttributes = getSelectedAttributes();
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tableName = NamingUtils.timestamp(ConsolidateWebVisit.name() + "_New");
        Table table = toTable(tableName, result.getTargets().get(0));
        log.info("Create new ConsolidateWebVisit table {}", tableName);
        metadataProxy.createTable(configuration.getCustomer(), tableName, table);
        exportToS3(table);
        dataUnitProxy.registerAthenaDataUnit(configuration.getCustomer(), tableName);
        dataCollectionProxy.upsertTable(customerSpace.toString(), tableName, ConsolidateWebVisit, inactive);
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

    private Integer toPartitionedDataUnit(String tableName, List<DataUnit> inputs) {
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        Table table = metadataProxy.getTable(configuration.getCustomer(), tableName);
        HdfsDataUnit du = table.partitionedToHdfsDataUnit("RawStream",
                Collections.singletonList(InterfaceName.StreamDateId.name()));
        inputs.add(du);
        return (inputs.size() - 1);
    }

    private Integer toDataUnit(String tableName, String alias, List<DataUnit> inputs) {
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        Table table = metadataProxy.getTable(configuration.getCustomer(), tableName);
        HdfsDataUnit du = table.toHdfsDataUnit(alias);
        inputs.add(du);
        return (inputs.size() - 1);
    }

    private String getPathPatternCatalogTable() {
        Catalog pathPatternCatalog = null;
        String catalogTableName = "";
        for (Catalog catalog : configuration.getCatalogs()) {
            if (catalog.getName().equals(EntityType.WebVisitPathPattern.name())) {
                pathPatternCatalog = catalog;
                break;
            }
        }
        if (pathPatternCatalog == null) {
            return catalogTableName;
        }
        //catalogId -> catalogTableName
        Map<String, String> catalogTables = getActiveTables(configuration.getCustomer(), BusinessEntity.Catalog,
                Collections.singletonList(pathPatternCatalog), Catalog::getCatalogId, ConsolidatedCatalog, inactive);
        if (MapUtils.isEmpty(catalogTables)) {
            catalogTables = getActiveTables(configuration.getCustomer(), BusinessEntity.Catalog,
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

    /*-
     * table names in current active version for WebVisit
     */
    private String getActiveWebVisitBatchStoreTable(@NotNull String customerSpace) {
        if (configuration.isRematchMode() || configuration.isReplaceMode()) {
            return "";
        }
        String webVisitBatchStoreTable = getStringValueFromContext(SSVI_WEBVISIT_RAW_TABLE);
        if (StringUtils.isNotEmpty(webVisitBatchStoreTable)) {
            return webVisitBatchStoreTable;
        }
        return dataCollectionProxy.getTableName(customerSpace, ConsolidateWebVisit, null);
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
        selectedAttributes.put("UtmSource", "utm_source");
        selectedAttributes.put("UtmMedium", "utm_medium");
        selectedAttributes.put("UtmCampaign", "utm_campaign");
        selectedAttributes.put("UtmTerm", "utm_term");
        selectedAttributes.put("UtmContent", "utm_content");
        return selectedAttributes;
    }

}
