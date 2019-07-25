package com.latticeengines.serviceflows.workflow.export;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importProcessAnalyzeFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportProcessAnalyzeFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportProcessAnalyzeFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        buildImportedRequests(requests);
        buildBusinessEntityRequests(requests);
        buildTablesForRetryRequests(requests);
    }

    @SuppressWarnings("rawtypes")
    private void buildImportedRequests(List<ImportExportRequest> requests) {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (MapUtils.isEmpty(entityImportsMap)) {
            log.info("There's no imported in the PA.");
            return;
        }
        for (Map.Entry<BusinessEntity, List> entry : entityImportsMap.entrySet()) {
            List<DataFeedImport> imports = JsonUtils.convertList(entry.getValue(), DataFeedImport.class);
            if (CollectionUtils.isNotEmpty(imports)) {
                List<Table> inputTables = imports.stream().map(DataFeedImport::getDataTable)
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(inputTables)) {
                    continue;
                }
                log.info("Imported business Entity, name=" + entry.getKey().name());
                for (Table table : inputTables) {
                    addTableToRequestForImport(table, requests);
                }
            }
        }

    }

    private void buildBusinessEntityRequests(List<ImportExportRequest> requests) {
        DataCollection.Version activeVersion = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        if (activeVersion == null) {
            log.info("There's no active version!");
            return;
        }
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            List<String> activeTableNames = dataCollectionProxy.getTableNames(customer, role, activeVersion);
            if (CollectionUtils.isNotEmpty(activeTableNames)) {
                log.info("Start to add active tables for tenant=" + customer + " role=" + role.name());
                activeTableNames.forEach(t -> addTableDir(t, requests));
            }
        }
    }

    private void buildTablesForRetryRequests(List<ImportExportRequest> requests) {
        Set<String> tableKeysForRetry = Sets.newHashSet( //
                ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
                ENTITY_MATCH_CONTACT_TARGETTABLE, //
                ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, //
                ACCOUNT_DIFF_TABLE_NAME, //
                ACCOUNT_MASTER_TABLE_NAME, //
                FULL_ACCOUNT_TABLE_NAME, //
                ACCOUNT_EXPORT_TABLE_NAME, //
                ACCOUNT_FEATURE_TABLE_NAME, //
                ACCOUNT_PROFILE_TABLE_NAME, //
                ACCOUNT_SERVING_TABLE_NAME, //
                ACCOUNT_STATS_TABLE_NAME
        );
        Set<String> tableNamesForRetry = tableKeysForRetry.stream() //
                .map(this::getStringValueFromContext) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(tableNamesForRetry)) {
            log.info("Start to add " + CollectionUtils.size(tableNamesForRetry) + " tables for retry for tenant=" + customer);
            tableNamesForRetry.forEach(t -> addTableDir(t, requests));
        }
    }

    private void addTableDir(String tableName, List<ImportExportRequest> requests) {
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        Table table = metadataProxy.getTable(customer, tableName);
        if (table == null) {
            log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            return;
        }
        addTableToRequestForImport(table, requests);
    }

}
