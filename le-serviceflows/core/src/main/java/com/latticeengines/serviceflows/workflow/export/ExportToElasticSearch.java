package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExportToElasticSearchJobConfig;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ExportToElasticSearchJob;

@Component(ExportToElasticSearch.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ExportToElasticSearch extends RunSparkJob<ExportToElasticSearchStepConfiguration, ExportToElasticSearchJobConfig> {

    private static Logger log = LoggerFactory.getLogger(ExportToElasticSearch.class);
    static final String BEAN_NAME = "exportToElasticSearch";

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private ServingStoreProxy servingStoreProxy;
    @Inject
    private ElasticSearchService elasticSearchService;

    private Map<String, String> entityWithESVersionMap;

    private Map<String, XContentBuilder> builderMap;

    private DataCollection.Version activeVersion;

    @Override
    protected Class<? extends AbstractSparkJob<ExportToElasticSearchJobConfig>> getJobClz() {
        return ExportToElasticSearchJob.class;
    }

    @Override
    protected ExportToElasticSearchJobConfig configureJob(ExportToElasticSearchStepConfiguration stepConfiguration) {
        Map<TableRoleInCollection, List> tableRoleInCollectionListMap =
                getMapObjectFromContext(TABLEROLES_GOING_TO_ES, TableRoleInCollection.class, List.class);
        if(MapUtils.isEmpty(tableRoleInCollectionListMap)) {
            log.info("can't find table, will skip export elasticsearch.");
            return null;
        }
        String originalTenant = configuration.getOriginalTenant();// provide tableRole table
        //activeVerion for current customerSpace which create tableRole table to es.
        activeVersion = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        if (activeVersion == null) {
            activeVersion = dataCollectionProxy.getActiveVersion(configuration.getCustomer());
        }
        log.info("Publish tableRole table from customer={} to customer={}", originalTenant,
                configuration.getCustomer());

        List<String> lookupIds = new ArrayList<>(ListUtils.emptyIfNull(servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account,
                        Collections.singletonList(ColumnSelection.Predefined.LookupId),
                        getInactiveVersion(configuration.getCustomer(), configuration.getOriginalTenant(), activeVersion))
                .map(ColumnMetadata::getAttrName).collectList().block()));
        // for testing
        lookupIds.add(InterfaceName.LatticeAccountId.name());
        log.info("lookupIds=" + lookupIds);

        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus == null) {
            dcStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                    activeVersion);
        }
        entityWithESVersionMap = new HashMap<>(MapUtils.emptyIfNull(dcStatus.getEntityToESVersionMap()));
        createIndex(tableRoleInCollectionListMap, configuration.getEsConfig(), configuration.getCustomer(), lookupIds);
        dcStatus.setEntityToESVersionMap(entityWithESVersionMap);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
        List<DataUnit> inputs = new ArrayList<>();
        ExportToElasticSearchJobConfig jobConfig = new ExportToElasticSearchJobConfig();
        jobConfig.esConfig =configuration.getEsConfig();
        jobConfig.customerSpace = configuration.getCustomer();
        jobConfig.entityWithESVersionMap = entityWithESVersionMap;
        toDataUnits(tableRoleInCollectionListMap, jobConfig.inputIdx, inputs);
        jobConfig.setInput(inputs);
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null) {
            dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), dcStatus, activeVersion);
        }
    }

    private void toDataUnits(Map<TableRoleInCollection, List> tableRoleInCollectionListMap,
            Map<String, List<Integer>> tableInputIdx, List<DataUnit> inputs) {
        if (MapUtils.isEmpty(tableRoleInCollectionListMap)) {
            return;
        }
        tableRoleInCollectionListMap.forEach((role, units) -> {
            List<HdfsDataUnit> tableUnits = JsonUtils.convertList(units, HdfsDataUnit.class);
            int startIdx = inputs.size();
            inputs.addAll(tableUnits);
            tableInputIdx.put(role.name(), Arrays.asList(startIdx, inputs.size()));
        });
    }

    private void createIndex(Map<TableRoleInCollection, List> tableRoleInCollectionListMap, ElasticSearchConfig esConfig,
                             String customerSpace, List<String> lookupIds) {
        if (MapUtils.isEmpty(tableRoleInCollectionListMap)) {
            return;
        }
        String newVersion = ElasticSearchUtils.generateNewVersion();
        log.info("tableRoleInCollectionListMap = {}", tableRoleInCollectionListMap);
        tableRoleInCollectionListMap.keySet().forEach(role -> {
            String entityKey = ElasticSearchUtils.getEntityFromTableRole(role);
            log.info("Create index for EsEntityType = {}, role = {}", entityKey, role);
            if (entityKey != null) {
                String idxName = String
                        .format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace), entityKey, newVersion)
                        .toLowerCase();
                entityWithESVersionMap.put(entityKey, newVersion);
                // TODO update mapping if changed
                if (BusinessEntity.Account.name().equals(entityKey)) {
                    elasticSearchService.createAccountIndexWithLookupIds(idxName, esConfig, lookupIds);
                } else {
                    elasticSearchService.createIndexWithSettings(idxName, esConfig, entityKey);
                }
            }
        });
    }

    private DataCollection.Version getInactiveVersion(String customerSpace, String originalTenant,
                                                      DataCollection.Version activeVersion) {
        if (originalTenant == null || customerSpace.equalsIgnoreCase(originalTenant)) {
            return activeVersion.complement();
        }
        return dataCollectionProxy.getInactiveVersion(originalTenant);
    }

}
