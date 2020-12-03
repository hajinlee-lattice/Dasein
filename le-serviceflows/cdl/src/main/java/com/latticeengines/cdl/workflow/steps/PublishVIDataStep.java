package com.latticeengines.cdl.workflow.steps;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilterValue;
import com.latticeengines.domain.exposed.elasticsearch.EsEntityType;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.PublishVIDataStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PublishVIDataJobConfiguration;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.config.ElasticSearchConfig;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DashboardProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.PublishVIDataJob;

@Component(PublishVIDataStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PublishVIDataStep extends RunSparkJob<PublishVIDataStepConfiguration, PublishVIDataJobConfiguration> {

    static final String BEAN_NAME = "publishViDataStep";
    private static Logger log = LoggerFactory.getLogger(PublishVIDataStep.class);
    private static final TypeReference<Map<String, Map<String, DimensionMetadata>>> METADATA_MAP_TYPE = new TypeReference<Map<String, Map<String, DimensionMetadata>>>() {
    };

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private ActivityStoreProxy activityStoreProxy;
    @Inject
    private DashboardProxy dashboardProxy;
    @Inject
    private ElasticSearchService elasticSearchService;

    private static final List<String> SELECTED_ATTRIBUTES = ImmutableList.of("AccountId", "WebVisitDate", "UserId",
            "WebVisitPageUrl", "SourceMedium", "LE_GlobalUlt_salesUSD", "LE_DomUlt_SalesUSD",
            "LE_GlobalULt_EmployeeTotal", "LE_DomUlt_EmployeeTotal", "LDC_DUNS", "DOMESTIC_ULTIMATE_DUNS_NUMBER", "GLOBAL_ULTIMATE_DUNS_NUMBER", "LE_SIC_CODE", "LE_Site_NAICS_Code", "LE_INDUSTRY", "LE_EMPLOYEE_RANGE", "LE_REVENUE_RANGE", "LE_IS_PRIMARY_DOMAIN", "LDC_Domain", "LDC_Name", "LDC_Country", "LDC_State", "LDC_City", "LE_DNB_TYPE", "UrlCategories");

    @Override
    protected Class<? extends AbstractSparkJob<PublishVIDataJobConfiguration>> getJobClz() {
        return PublishVIDataJob.class;
    }

    @Override
    protected PublishVIDataJobConfiguration configureJob(PublishVIDataStepConfiguration stepConfiguration) {
        Table latticeAccountTable = dataCollectionProxy.getTable(configuration.getCustomer(), TableRoleInCollection.LatticeAccount,
                configuration.getVersion());
        Map<String, String> webVisitTableNameIsMaps = getRawStreamTableNames();
        if (MapUtils.isEmpty(webVisitTableNameIsMaps) || latticeAccountTable == null) {
            log.info("webVisitTableNameIsMaps is empty or latticeAccountTable is null, skip this step.");
            return null;
        }
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus == null) {
            dcStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                    configuration.getVersion());
        }
        Map<String, String> entityWithESVersionMap =
                new HashMap<>(MapUtils.emptyIfNull(dcStatus.getEntityWithESVersionMap()));
        String index = createIndex(configuration.getCustomer(), entityWithESVersionMap);
        dcStatus.setEntityWithESVersionMap(entityWithESVersionMap);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
        List<DataUnit> inputs = new ArrayList<>();
        ElasticSearchConfig esConfig = elasticSearchService.getDefaultElasticSearchConfig();
        PublishVIDataJobConfiguration config = new PublishVIDataJobConfiguration();
        toDataUnits(new ArrayList<>(webVisitTableNameIsMaps.values()), config.inputIdx, inputs);
        config.latticeAccountTableIdx = inputs.size();
        config.targetNum = 0;
        config.isTest = false;
        config.webVisitTableNameIsMaps = webVisitTableNameIsMaps;
        inputs.add(latticeAccountTable.toHdfsDataUnit("LatticeAccount"));
        config.selectedAttributes = SELECTED_ATTRIBUTES;
        config.esConfigs.put("esHost", esConfig.getEsHost());
        config.esConfigs.put("esPorts", esConfig.getEsPort());
        config.esConfigs.put("user", esConfig.getEsUser());
        config.esConfigs.put("pwd", esConfig.getEsPassword());
        config.esConfigs.put("esIndex", index);
        config.filterParams = getFilterParams();
        // set dimensions
        config.dimensionMetadataMap = getTypedObjectFromContext(STREAM_DIMENSION_METADATA_MAP, METADATA_MAP_TYPE);
        if (config.dimensionMetadataMap == null) {
            config.dimensionMetadataMap = activityStoreProxy.getDimensionMetadata(customerSpace.toString(), null);
        }
        if (MapUtils.isEmpty(config.dimensionMetadataMap)) {
            log.info("can't find the DimensionMetadata, will skip publish VI Data.");
            return null;
        }
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null) {
            dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), dcStatus, configuration.getVersion());
        }
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, List> filterParamMaps = JsonUtils.convertMap(rawMap, String.class, List.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(filterParamMaps),
                "publish VIData output filterParam map should not be empty here");
        List<DashboardFilter> filterList = new ArrayList<>();
        for (Map.Entry<String, List> entry : filterParamMaps.entrySet()) {
            DashboardFilter filter = new DashboardFilter();
            filter.setName(entry.getKey());
            List<String> filterValues = JsonUtils.convertList(entry.getValue(), String.class);
            List<DashboardFilterValue> filterValueList = new ArrayList<>();
            for (String filterValue : filterValues) {
                DashboardFilterValue value = new DashboardFilterValue();
                value.setDisplayName(filterValue);
                value.setValue(filterValue);
                filterValueList.add(value);
            }
            filter.setFilterValue(filterValueList);
            filterList.add(filter);
        }
        dashboardProxy.createDashboardFilterList(configuration.getCustomer(), filterList);
    }

    private Map<String, String> getRawStreamTableNames() {
        List<AtlasStream> streams = activityStoreProxy.getStreams(configuration.getCustomer());
        List<String> webVisitStreamIds = streams.stream()
                .filter(stream -> (stream.getStreamType() == AtlasStream.StreamType.WebVisit)).map(AtlasStream::getStreamId)
        .collect(Collectors.toList());
        return dataCollectionProxy.getTableNamesWithSignatures(configuration.getCustomer(),
                TableRoleInCollection.ConsolidatedActivityStream, configuration.getVersion(), webVisitStreamIds);
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

    private String createIndex(String customerSpace, Map<String, String> entityWithESVersionMap) {
        String newVersion = generateNewVersion();
        String idxName = String
                .format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace), EsEntityType.VIData, newVersion)
                .toLowerCase();
        entityWithESVersionMap.put(EsEntityType.VIData.name(), newVersion);
        elasticSearchService.createIndex(idxName, EsEntityType.VIData);
        return idxName;
    }


    private String generateNewVersion() {
        return String.valueOf(Instant.now().toEpochMilli());
    }

    private Map<String, String> getFilterParams() {
        Map<String, String> filterParams = new HashMap<>();
        filterParams.put("<REVENUE_FILTER>", "LE_REVENUE_RANGE");
        filterParams.put("<INDUSTRY_FILTER>", "LE_INDUSTRY");
        filterParams.put("<LOCATION_FILTER>", "LDC_State");
        filterParams.put("<PAGE_FILTER>", "UrlCategories");
        filterParams.put("<EMPLOYEE_FILTER>", "LE_EMPLOYEE_RANGE");
        return filterParams;
    }
}
